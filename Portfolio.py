import poloniex
import asyncio
import signal
import functools

from requests.exceptions import RequestException
from threading import RLock
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty
from time import time
from collections import defaultdict


class PortfolioEvent(object):
    # Basic event object
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

    def __init__(self, poloObj, initialPairsWeights, marketBuyPercentage, logger, isSimulation=False, simulationAmount=1.0, pairs=None):
        if not poloObj.key:
            raise PortfolioMisconfiguredException("No key in poloObj")
        # amount to market buy every round
        self.marketBuyPercentage = marketBuyPercentage
        self.polo = poloObj
        self.waitTimeBetweenOrders = self.polo.MINUTE
        self.isSimulation = isSimulation
        if self.isSimulation:
            self.amount = simulationAmount
            self.balances = defaultdict(int)
            self.balances["BTC"] = self.amount
        else:
            self.amount = None
        self.logger = logger
        if self.logger["LOG_TRANSACTION"]:
            self.logger.writeToFile(
                "LOG_TRANSACTION", "type,pair,amount,price,btc,date")
        if self.logger["LOG_AMOUNT"]:
            self.logger.writeToFile(
                "LOG_AMOUNT", "amount,date")

        self.buySellRetryTimes = 10
        # the more sell coroutines there are, the slower the buy coroutines
        # retry duration
        self.buyRetryDuration = 1.0
        self.eventLoopSleepTime = 30.0

        self.mainLoop = asyncio.get_event_loop()
        self.buyTasks = []
        self.sellTasks = []
        # for cancellation upon shutdown
        self.pairsOnOrder = set()

        self.orderSetLock = RLock()
        self.buyLock = RLock()
        self.sellLock = RLock()
        self.cancelLock = RLock()
        self.buyRetryDurationLock = RLock()

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
                    event = self.eventQueue.get_nowait()
                except Empty:
                    await asyncio.sleep(self.eventLoopSleepTime)
                    continue

                self.logger.debug(
                    "Event: {} obtained from event queue".format(event))

                if isinstance(event, PortfolioEvent):
                    if isinstance(event, StartEvent):
                        # update pairs, for cancellation purposes
                        self.pairs = event.pairsWeights.keys()

                    elif isinstance(event, ReconfigureEvent):
                        # update pairs, for cancellation purposes
                        self.pairs = event.pairsWeights.keys()

                        # cancel all current buy and sell routines
                        with self.cancelLock:
                            with self.buyLock:
                                await self.__cancelTasks(self.buyTasks)
                                self.buyTasks = []
                            with self.sellLock:
                                await self.__cancelTasks(self.sellTasks)
                                self.sellTasks = []

                        # reset orders set (for cancellation upon shutdown)
                        with self.orderSetLock:
                            self.pairsOnOrder = set()

                    if self.isSimulation:
                        previousPairAmount = self.balances
                        self.amount = sum(self.balances.values())
                        if self.logger["LOG_AMOUNT"]:
                            self.logger.writeToFile("LOG_AMOUNT", str(
                                self.amount) + "," + str(time()))
                    else:
                        while True:
                            balances = self.polo.returnCompleteBalances()
                            if "error" in balances:
                                self.logger.info(
                                    "Error in _eventRun: can't obtain complete balances")
                            else:
                                break

                        # get previous pair amount
                        previousPairAmount = {}
                        currentAmount = float(balances["BTC"]["btcValue"])
                        for pair in self.pairs:
                            ticker = pair[4:]
                            if ticker in balances:
                                previousPairAmount[pair] = float(
                                    balances[ticker]["btcValue"])
                                currentAmount += float(
                                    balances[ticker]["btcValue"])
                            else:
                                self.logger.debug(
                                    "previousPairAmount computation: ticker {} not in balances. Pair: {}".format(ticker, pair))

                        # update self.amount for logging purposes
                        self.amount = currentAmount

                    if isinstance(event, StartEvent):
                        self.logger.debug(
                            "Starting account value: {}".format(self.amount))
                    elif isinstance(event, ReconfigureEvent):
                        self.logger.debug(
                            "Current account value: {}".format(self.amount))

                    currentPairAmount = Portfolio.getPairAmount(
                        self.amount, event.pairsWeights)

                    pairAmountDifference = Portfolio.getPairAmountDifference(
                        currentPairAmount, previousPairAmount)
                    self.logger.debug("Pair amount difference {}:".format(
                        {k: pairAmountDifference[k] for k in pairAmountDifference if abs(pairAmountDifference[k]) != 0.0}))

                    # buy or sell base on amount differences
                    buyPairs = {}
                    sellPairs = {}
                    for (pair, diff) in pairAmountDifference.items():
                        if diff > 0:
                            buyPairs[pair] = diff
                        elif diff < 0:
                            sellPairs[pair] = -diff
                    self.mainLoop.create_task(self.__sellAllPairs(sellPairs))
                    self.mainLoop.create_task(self.__buyAllPairs(buyPairs))

        except asyncio.CancelledError:
            # LOG
            self.logger.debug(
                ">> Coroutine: __eventRun cancelled")

    @staticmethod
    def getPairAmountDifference(currentPairAmount, prevPairAmount):
        pairAmountDifference = {}
        for (pair, amount) in currentPairAmount.items():
            pairAmountDifference[pair] = amount - \
                prevPairAmount.get(pair, 0.0)
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
            await self.__cancelTasks(tasks=tasks)

            # cancel all orders
            if not self.isSimulation:
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
    def determinePrices(orderBookBids, orderBookAsks,
                        marketBuyPercentage, orderType="buy"):
        # returns prices to order at and weights
        # [(price, weight),...]
        ordersToPut = []
        remainingPercentage = (1.0 - marketBuyPercentage)
        # (priceToBid, weight)
        if orderType == "buy":
            ordersToPut.append(
                (orderBookAsks[0][0] * 1.001, marketBuyPercentage))
            ordersToPut.append(
                (orderBookBids[0][0] * 1.000, 0.1 * remainingPercentage))
            ordersToPut.append(
                (orderBookBids[1][0] * 1.001, 0.1 * remainingPercentage))
            ordersToPut.append(
                (orderBookBids[1][0] * 1.001, 0.25 * remainingPercentage))
            ordersToPut.append(
                (orderBookBids[2][0] * 1.002, 0.2 * remainingPercentage))
            ordersToPut.append(
                (orderBookBids[2][0] * 1.001, 0.25 * remainingPercentage))
            ordersToPut.append(
                (orderBookBids[2][0] * 1.00, 0.1 * remainingPercentage))
        if orderType == "sell":
            ordersToPut.append(
                (orderBookBids[0][0] * 0.999, marketBuyPercentage))
            ordersToPut.append(
                (orderBookAsks[1][0] * 0.997, 0.2 * remainingPercentage))
            ordersToPut.append(
                (orderBookAsks[1][0] * 0.998, 0.2 * remainingPercentage))
            ordersToPut.append(
                (orderBookAsks[1][0] * 0.999, 0.2 * remainingPercentage))
            ordersToPut.append(
                (orderBookAsks[2][0] * 0.998, 0.2 * remainingPercentage))
            ordersToPut.append(
                (orderBookAsks[2][0] * 0.999, 0.2 * remainingPercentage))
            # (priceToBid, weight)
        return ordersToPut

    async def __cancelOrder(self, pair, orderType="all", withYield=True):
        # cancels all orders for a given pair
        # and returns amount on open orders
        numberOfOpenOrders = 0
        retryCancellation = True
        retryMaxCount = 10
        while retryCancellation is not False:
            retryCancellation = False
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
                numberOfOpenOrders += 1
                retry = True
                retryCount = 0
                while retry is not False:
                    retry = False
                    try:
                        cancellationMessage = self.polo.cancelOrder(orderNo)
                        if cancellationMessage["success"] != 1 or "error" in cancellationMessage:
                            retry = True
                            if retryCount >= retryMaxCount:
                                retryCancellation = True
                                break
                            retryCount += 1
                            self.logger.debug(
                                "Cancellation of order for {} failed".format(pair))
                            if withYield:
                                await asyncio.sleep(1.0)
                            continue
                        self.logger.debug(
                            "Cancellation of order for {} successful".format(pair))
                    except Exception:
                        retry = True
                        if retryCount >= retryMaxCount:
                            retryCancellation = True
                            break
                        retryCount += 1
                        self.logger.debug(
                            "Cancellation of order for {} failed".format(pair))
                        if withYield:
                            await asyncio.sleep(1.0)
        return numberOfOpenOrders

    async def __sellCoroutine(self, pair, amountToSell):
        amountLeftToOrder = amountToSell
        self.logger.debug(
            "SELL: amount to play for {}: {}".format(pair, amountToSell))
        # increase buy coroutine retry duration to prevent hogging of CPU by buy coroutines
        # that cannot buy due to insufficient funds
        with self.buyRetryDurationLock:
            self.buyRetryDuration *= 1.5

        try:
            while True:
                # {"asks":[[0.00007600,1164],[0.00007620,1300], ... ],
                #  "bids":[[0.00006901,200],[0.00006900,408], ... ], "isFrozen": 0, "seq": 18849}
                orderBook = self.polo.returnOrderBook(pair, depth=100)
                orderBookBids = list(map(lambda tup: tuple(map(float, tup)),
                                         orderBook["bids"]))

                orderBookAsks = list(map(lambda tup: tuple(map(float, tup)),
                                         orderBook["asks"]))

                # determine price and corresponding amount to order
                # if the value put on order < 0.0001, stack the order with the
                # next
                actualWeight = 0
                pricesAndAmount = []
                grandTotal = 0
                for (price, weight) in Portfolio.determinePrices(orderBookBids, orderBookAsks,
                                                                 self.marketBuyPercentage, "sell"):
                    # if total < 0.0001, stack the pair with the next
                    actualWeight += weight
                    total = actualWeight * amountLeftToOrder * 0.99
                    grandTotal += total
                    if total > 0.0001:
                        pricesAndAmount.append((price, total / price))
                        # reset actualWeight for the next pair
                        actualWeight = 0

                if grandTotal <= 0.0001:
                    break

                for (price, amt) in pricesAndAmount:
                    retry = True
                    # place bids
                    if self.isSimulation:
                        self.logger.debug(
                            "{} of {} bought at {} - amt in btc: {}".format(amt, pair, price, price * amt))
                        if self.logger["LOG_TRANSACTION"]:
                            self.logger.writeToFile("LOG_TRANSACTION", "sell,{},{},{},{},{}".format(
                                pair, amt, price, price * amt, time()))
                        self.balances["BTC"] += price * amt
                        self.balances[pair] -= price * amt
                    else:
                        while retry is not False:
                            retry = False
                            try:
                                # {"orderNumber":31226040,"resultingTrades":[{"amount":"338.8732","date":"2014-10-18 23:03:21","rate":"0.00000173","total":"0.00058625","tradeID":"16164","type":"buy"}]}
                                order = self.polo.sell(
                                    pair, price, amt)
                                if "error" in order:
                                    self.logger.debug(
                                        "Error: {}".format(order["error"]))
                                    self.logger.debug(
                                        "Retrying sell bidding for {}: price {} amt {}".format(pair, price, amt))
                                    retry = True
                                    await asyncio.sleep(1.0)
                                    continue
                                else:
                                    self.logger.debug(
                                        "Sell order placed for {}".format(pair))
                            except Exception as e:
                                self.logger.debug(e)
                                if str(e).startswith("Total must be at least"):
                                    break
                                amt = amt * 0.5
                                self.logger.debug(
                                    "Retrying sell bidding for {}: price {} amt {}".format(pair, price, amt))
                                retry = True
                                await asyncio.sleep(1.0)
                                continue
                                # cancel bids and retry

                if self.isSimulation:
                    break

                await asyncio.sleep(self.waitTimeBetweenOrders)

                # cancel all orders and
                # update amountLeftToOrder
                numberOfOrdersCancelled = await self.__cancelOrder(pair, "sell")
                # stop coroutine since everything is filled
                if (numberOfOrdersCancelled <= 0):
                    break

            # amountToSell all went through into orders
            self.logger.debug(
                "SELL: orders for {} all went through".format(pair))

            # reduce buy duration once sell coroutine is done selling
            with self.buyRetryDurationLock:
                self.buyRetryDuration /= 1.5

        except asyncio.CancelledError:
            await self.__cancelOrder(pair, "sell")
            # LOG
            self.logger.debug(
                ">> Coroutine: __sellCoroutine for {} cancelled".format(pair))

    async def __buyCoroutine(self, pair, amountToPlay):
        amountLeftToOrder = amountToPlay
        self.logger.debug(
            "BUY: amount to play for {}: {}".format(pair, amountToPlay))
        try:
            # if retry times more than N, break coroutine
            while True:
                    # {"asks":[[0.00007600,1164],[0.00007620,1300], ... ],
                    #  "bids":[[0.00006901,200],[0.00006900,408], ... ], "isFrozen": 0, "seq": 18849}
                orderBook = self.polo.returnOrderBook(pair, depth=100)
                orderBookBids = list(map(lambda tup: tuple(map(float, tup)),
                                         orderBook["bids"]))

                orderBookAsks = list(map(lambda tup: tuple(map(float, tup)),
                                         orderBook["asks"]))

                # determine price and corresponding amount to order
                # if the value put on order < 0.0001, stack the order with the
                # next
                actualWeight = 0
                grandTotal = 0
                pricesAndAmount = []
                for (price, weight) in Portfolio.determinePrices(orderBookBids, orderBookAsks,
                                                                 self.marketBuyPercentage, "buy"):
                    # if total < 0.0001, stack the pair with the next
                    actualWeight += weight
                    total = actualWeight * amountLeftToOrder * 0.99
                    grandTotal += total
                    if total > 0.0001:
                        pricesAndAmount.append((price, total / price))
                        # reset actualWeight for the next pair
                        actualWeight = 0

                if grandTotal <= 0.0001:
                    break

                for (price, amt) in pricesAndAmount:
                    retry = True
                    # place bids
                    if self.isSimulation:
                        self.logger.debug(
                            "{} of {} bought at {} - amt in btc: {}".format(amt, pair, price, price * amt))
                        if self.logger["LOG_TRANSACTION"]:
                            self.logger.writeToFile("LOG_TRANSACTION", "buy,{},{},{},{},{}".format(
                                pair, amt, price, price * amt, time()))
                        self.balances["BTC"] -= price * amt
                        self.balances[pair] += price * amt
                    else:
                        while retry is not False:
                            retry = False
                            try:
                                # {"orderNumber":31226040,"resultingTrades":[{"amount":"338.8732","date":"2014-10-18 23:03:21","rate":"0.00000173","total":"0.00058625","tradeID":"16164","type":"buy"}]}
                                order = self.polo.buy(
                                    pair, price, amt)
                                if "error" in order:
                                    self.logger.debug(
                                        "Error: {}".format(order["error"]))
                                    self.logger.debug(
                                        "Retrying buy bidding for {}: price {} amt {}".format(pair, price, amt))
                                    retry = True
                                    await asyncio.sleep(self.buyRetryDuration)
                                    continue
                                else:
                                    self.logger.debug(
                                        "Buy order placed for {}".format(pair))
                            except Exception as e:
                                self.logger.debug(e)
                                if str(e).startswith("Total must be at least"):
                                    break
                                amt = amt * 0.5
                                self.logger.debug(
                                    "Retrying buy bidding for {}: price {} amt {}".format(pair, price, amt))
                                retry = True
                                await asyncio.sleep(self.buyRetryDuration)
                                continue
                            # cancel bids and retry
                if self.isSimulation:
                    break

                await asyncio.sleep(self.waitTimeBetweenOrders)

                # cancel all orders and
                # update amountLeftToOrder

                numberOfOrdersCancelled = await self.__cancelOrder(pair, "buy")
                # stop coroutine since everything is filled
                if (numberOfOrdersCancelled <= 0):
                    break

            # amountToPlay all went through into orders
            self.logger.debug(
                "BUY: orders for {} all went through".format(pair))
        except asyncio.CancelledError:
            await self.__cancelOrder(pair, "buy")
            # LOG
            self.logger.debug(
                ">> Coroutine: __buyCoroutine for {} cancelled".format(pair))
