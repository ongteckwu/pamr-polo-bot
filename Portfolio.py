import poloniex
import asyncio
import logging
import signal
import functools

from requests.exceptions import RequestException
from threading import RLock
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty


class PortfolioEvent(object):
    pass


class ReconfigureEvent(PortfolioEvent):

    def __init__(self, pairsWeights):
        assert(0.99 <= sum(pairsWeights.values()) <= 1.01,
               "Weights do not sum up to 1")
        self.pairsWeights = pairsWeights

    def __str__(self):
        return "ReconfigureEvent(PortfolioEvent)"


class StartEvent(PortfolioEvent):

    def __init__(self, pairsWeights):
        assert(0.99 <= sum(pairsWeights.values()) <= 1.01,
               "Weights do not sum up to 1")
        self.pairsWeights = pairsWeights

    def __str__(self):
        return "StartEvent(PortfolioEvent)"


class Portfolio(object):

    class PortfolioMisconfiguredException(Exception):
        pass

    def __init__(self, poloObj, initialAmount, initialPairsWeights, pairs=None, isSimulation=False):
        # if not poloObj.key:
        #   raise PortfolioMisconfiguredException("No key in poloObj")
        self.polo = poloObj
        self.amount = initialAmount
        self.waitTimeBetweenOrders = self.polo.MINUTE
        self.isSimulation = isSimulation

        self.mainLoop = asyncio.get_event_loop()
        self.buyTasks = []
        self.sellTasks = []
        self.pairsOnOrder = set()

        self.orderSetLock = RLock()
        self.buyLock = RLock()
        self.sellLock = RLock()
        self.cancelLock = RLock()

        self.eventQueue = Queue()
        # default pairs for the purpose of cancellation
        # on loop stop if pairs not provided
        self.pairs = pairs if pairs is not None else [a for a in
                                                      self.polo.returnTicker().keys() if a.startswith("BTC")]  # updated only by _eventRun

        # self.mainLoop.add_signal_handler(signal.SIGINT,
        #                                  functools.partial(asyncio.ensure_future,
        #                                                    self.stop()))
        # self.mainLoop.add_signal_handler(signal.SIGTERM,
        #                                  functools.partial(asyncio.ensure_future,
        #                                                    self.stop()))
        self.mainLoop.create_task(self._eventRun())
        self.eventQueue.put_nowait(
            StartEvent(initialPairsWeights))

    def sendEvent(self, event):
        self.eventQueue.put_nowait(event)

    async def _eventRun(self):
        # currentAmount == prevAmount => doNothing
        # prevAmount == 0 && prevAmount < currentAmount => buy
        # prevAmount != 0 && prevAmount < currentAmount => buyTillComplete
        # currentAmount == 0 && prevAmount > currentAmount => sellAll
        # currentAmount != 0 && prevAmount > currentAmount => sellPartial
        try:
            while True:
                try:
                    event = self.eventQueue.get()
                except Empty:
                    logging.debug("Event Run loop sleeping...")
                    await asyncio.sleep(30)
                    continue

                logging.debug(
                    "Event: {} obtained from event queue".format(event))

                if isinstance(event, PortfolioEvent):
                    if isinstance(event, StartEvent):
                        # update pairs, for cancellation purposes
                        self.pairs = event.pairsWeights.keys()
                        await self.__buyAllPairs(Portfolio.getPairAmount(
                            self.amount, event.pairsWeights))

                    elif isinstance(event, ReconfigureEvent):
                        # update pairs, for cancellation purposes
                        self.pairs = event.pairsWeights.keys()

                        # cancel all current buy and sell routines
                        with self.cancelLock:
                            with buyLock:
                                await self.__cancelTasks(self.buyTasks)
                                self.buyTasks = []
                            with sellLock:
                                await self.__cancelTasks(self.sellTasks)
                                self.sellTasks = []

                        # reset orders set
                        with self.orderSetLock:
                            self.pairsOnOrder = set()

                        # get current balance in btc value
                        while True:
                            balances = self.polo.returnCompleteBalances()
                            if "error" in balances:
                                logging.debug(
                                    "Error in _eventRun: can't obtain complete balances")
                            else:
                                break

                        # get previous pair amount
                        previousPairAmount = {}
                        currentAmount = balances["BTC"]["available"]
                        for pair in self.pairs:
                            ticker = pair[-3:]
                            if ticker in balances:
                                previousPairAmount[pair] = balances[
                                    ticker]["btcValue"]
                                currentAmount += balances[ticker]["btcValue"]
                            else:
                                logging.debug(
                                    "previousPairAmount computation: ticker {} not in balances. Pair: {}".format(ticker, pair))

                        # update self.amount for logging purposes
                        self.amount = currentAmount
                        logging.debug(
                            "Current account value: {}".format(self.amount))

                        currentPairAmount = Portfolio.getPairAmount(
                            self.amount, event.pairsWeights)

                        pairAmountDifference = Portfolio.getPairAmountDifference(
                            currentPairAmount, previousPairAmount)
                        logging.debug("Pair amount difference {}:".format(
                            pairAmountDifference))

                        # buy or sell base on amount differences
                        buyPairs = {}
                        sellPairs = {}
                        for (pair, diff) in pairAmountDifference.items():
                            if diff > 0:
                                buyPairs[pair] = diff
                            elif diff < 0:
                                sellPair[pair] = -diff
                        await self.__buyAllPairs(buyPairs)
                        await self.__sellAllPairs(sellPairs)
        except asyncio.CancelledError:
            # LOG
            logging.debug(
                ">> Coroutine: __eventRun cancelled")

    @staticmethod
    def getPairAmountDifference(currentPairAmount, prevPairAmount):
        pairAmountDifference = {}
        for (pair, amount) in currentPairAmount.items():
            pairAmountDifference[pair] = amount - \
                prevPairAmount.get(pair, default=0.0)
        return pairAmountDifference

    async def __buyAllPairs(self, pairAmount):
        buyTasks = []
        pairsOnOrder = []
        for (pair, amount) in pairAmount.items():
            if amount > 0.0:
                pairsOnOrder.append(pair)
                task = self.mainLoop.create_task(
                    self.__buyCoroutine(pair, amount))
                buyTasks.append(task)

        # add to global buyTasks list for cancellation purposes
        with self.buyLock:
            self.buyTasks.extend(buyTasks)

        with self.orderSetLock:
            self.pairsOnOrder.update(pairsOnOrder)
        await asyncio.gather(*buyTasks)

    async def __sellAllPairs(self, pairAmount):
        sellTasks = []
        pairsOnOrder = []
        for (pair, amount) in pairAmount.items():
            if amount > 0.0:
                pairsOnOrder.append(pair)
                task = self.mainLoop.create_task(
                    self.__sellCoroutine(pair, amount))
                sellTasks.append(task)

        # add to global sellTasks list for cancellation purposes
        with self.sellLock:
            self.sellTasks.extend(sellTasks)

        with self.orderSetLock:
            self.pairsOnOrder.update(pairsOnOrder)
        await asyncio.gather(*sellTasks)

    async def __cancelTasks(self, tasks):
        print("Cancelling tasks...")
        list(map(lambda task: task.cancel(), tasks))
        await asyncio.gather(*tasks, return_exceptions=True)
        print("ALL TASKS CANCELLED")

    async def stop(self):
        print("Signal handler activated")
        # stops the event loop and all other loops

        with self.cancelLock:
            tasks = [task for task in asyncio.Task.all_tasks() if task is not
                     asyncio.tasks.Task.current_task()]
            logging.debug(tasks)
            await self.__cancelTasks(tasks=tasks)

            # cancel all orders
            print("Cancelling all orders...")
            for pair in self.pairsOnOrder:
                print("Cancelling orders on {}...".format(pair))
                await self.__cancelOrder(pair)

            self.mainLoop.stop()

    @staticmethod
    def getPairAmount(amount, pairsWeights):
        # get pair-amount in BTC
        return {pair: (pairsWeights[pair] * amount) for pair in pairsWeights}

    @staticmethod
    def determinePrices(orderBook, orderType="buy"):
        # returns prices to order at and weights
        # [(price, weight),...]
        ordersToPut = []
        # buy if orderType==0, sell if orderType==1
        # (priceToBid, weight)
        # ordersToPut.append((orderBook[0][0], 0.1))
        # # (priceToBid, weight)
        # ordersToPut.append((orderBook[4][0], 0.1))
        # # (priceToBid, weight)
        # ordersToPut.append((orderBook[6][0], 0.2))
        # # (priceToBid, weight)
        # ordersToPut.append((orderBook[8][0], 0.2))
        # # (priceToBid, weight)
        # ordersToPut.append((orderBook[10][0], 0.2))
        # (priceToBid, weight)
        # ordersToPut.append((orderBook[12][0], 0.2))
        ordersToPut.append((orderBook[19][0], 1.0))
        assert(sum([pair[1] for pair in ordersToPut]) == 1.0,
               "Weights on determinePrices do not sum up to 1")
        return ordersToPut

    async def __cancelOrder(self, pair, orderType="all", withYield=True):
        # cancels all orders for a given pair
        # and returns amount on open orders
        amountOnOpenOrders = 0
        while True:
            openOrders = self.polo.returnOpenOrders(pair)
            if "error" in openOrders:
                await asyncio.sleep(1.0)
            else:
                break

        for order in openOrders:
            # filter order types
            if orderType == "all":
                pass
            elif orderType == "buy":
                if order["type"] != "buy":
                    continue
            elif orderType == "sell":
                if order["type"] != "sell":
                    continue

            orderNo = order["orderNumber"]
            amountOnOpenOrders += float(order["total"])
            retry = False
            while retry is False:
                try:
                    cancellationMessage = self.polo.cancelOrder(orderNo)
                    if cancellationMessage["success"] != 1 or "error" in cancellationMessage:
                        retry = True
                        logging.debug(
                            "Cancellation of order for {} failed".format(pair))
                        if withYield:
                            await asyncio.sleep(1.0)
                        continue
                    logging.debug(
                        "Cancellation of order for {} successful".format(pair))
                except Exception:
                    retry = True
                    logging.debug(
                        "Cancellation of order for {} failed".format(pair))
                    if withYield:
                        await asyncio.sleep(1.0)
        return amountOnOpenOrders

    async def __sellCoroutine(self, pair, amountToSell):
        amountLeftToOrder = amountToSell
        logging.debug(
            "SELL: amount to play for {}: {}".format(pair, amountToSell))
        try:
            while (amountLeftToOrder >= amountToSell * 0.01):
                # {"asks":[[0.00007600,1164],[0.00007620,1300], ... ],
                #  "bids":[[0.00006901,200],[0.00006900,408], ... ], "isFrozen": 0, "seq": 18849}
                orderBook = list(map(lambda tup: tuple(
                    map(float, tup)), self.polo.returnOrderBook(pair, depth=100)["asks"]))
                # determine prices in the specific altcoin
                pricesAndAmount = [(price, (weight * amountLeftToOrder) / price)
                                   for (price, weight) in Portfolio.determinePrices(orderBook, "sell")]
                for (price, amt) in pricesAndAmount:
                    retry = True
                    # place bids
                    while retry is not False:
                        retry = False
                        try:
                            # {"orderNumber":31226040,"resultingTrades":[{"amount":"338.8732","date":"2014-10-18 23:03:21","rate":"0.00000173","total":"0.00058625","tradeID":"16164","type":"buy"}]}
                            order = self.polo.sell(pair, price, amt)
                            if "error" in order:
                                logging.debug(
                                    "Error: {}".format(order["error"]))
                                logging.debug(
                                    "Retrying sell bidding for {}".format(pair))
                                retry = True
                                await asyncio.sleep(1.0)
                                continue
                            else:
                                logging.debug(
                                    "Sell order placed for {}".format(pair))
                        except RequestException:
                            logging.debug(
                                "Retrying sell bidding for {}".format(pair))
                            retry = True
                            await asyncio.sleep(1.0)
                            continue
                            # cancel bids and retry

                await asyncio.sleep(self.waitTimeBetweenOrders)

                # cancel all orders and
                # update amountLeftToOrder
                amountLeftToOrder = await self.__cancelOrder(pair, "sell")
            # amountToSell all went through into orders
            logging.debug("SELL: orders for {} all went through".format(pair))
        except asyncio.CancelledError:
            # LOG
            logging.debug(
                ">> Coroutine: __sellCoroutine for {} cancelled".format(pair))

    async def __buyCoroutine(self, pair, amountToPlay):
        amountLeftToOrder = amountToPlay
        logging.debug(
            "BUY: amount to play for {}: {}".format(pair, amountToPlay))
        try:
            while (amountLeftToOrder >= amountToPlay * 0.05):
                # {"asks":[[0.00007600,1164],[0.00007620,1300], ... ],
                #  "bids":[[0.00006901,200],[0.00006900,408], ... ], "isFrozen": 0, "seq": 18849}
                orderBook = list(map(lambda tup: tuple(map(float, tup)),
                                     self.polo.returnOrderBook(pair, depth=100)["bids"]))
                # determine prices in the specific altcoin
                pricesAndAmount = [(price, ((weight * amountLeftToOrder) / price) * 0.99)
                                   for (price, weight) in Portfolio.determinePrices(orderBook, "buy")]
                for (price, amt) in pricesAndAmount:
                    retry = True
                    # place bids
                    print(price, amt)
                    while retry is not False:
                        retry = False
                        try:
                            # {"orderNumber":31226040,"resultingTrades":[{"amount":"338.8732","date":"2014-10-18 23:03:21","rate":"0.00000173","total":"0.00058625","tradeID":"16164","type":"buy"}]}
                            order = self.polo.buy(pair, price, amt)
                            logging.debug(order)
                            if "error" in order:
                                logging.debug(
                                    "Error: {}".format(order["error"]))
                                logging.debug(
                                    "Retrying buy bidding for {}".format(pair))
                                retry = True
                                await asyncio.sleep(1.0)
                                continue
                            else:
                                logging.debug(
                                    "Buy order placed for {}".format(pair))
                        except Exception as e:
                            logging.debug(e)
                            logging.debug(
                                "Retrying buy bidding for {}".format(pair))
                            retry = True
                            await asyncio.sleep(1.0)
                            continue
                            # cancel bids and retry
                logging.debug("Buy coroutine for {} sleeping...".format(pair))
                await asyncio.sleep(self.waitTimeBetweenOrders)

                # cancel all orders and
                # update amountLeftToOrder
                amountLeftToOrder = await self.__cancelOrder(pair, "buy")
            # amountToPlay all went through into orders
            logging.debug("BUY: orders for {} all went through".format(pair))
        except asyncio.CancelledError:
            # LOG
            logging.debug(
                ">> Coroutine: __buyCoroutine for {} cancelled".format(pair))
