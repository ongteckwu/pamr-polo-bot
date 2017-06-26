import poloniex
import pandas as pd
import logging
import asyncio
import sys
import ratioStrategy


from pprint import pprint
from time import time, sleep
from pandas import DataFrame
from PAMR import PAMR
from utilities import cleanNANs, pairsToWeights
from Portfolio import Portfolio, ReconfigureEvent


log = logging.getLogger(__name__)
logPortfolio = logging.getLogger(Portfolio.__name__)
handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.INFO)
log.addHandler(handler)
logPortfolio.addHandler(handler)

# GET ALL PAIRS
API_KEY = "8N0DP32H-UDZX8K7F-Q70NU866-BD501GE0"
SECRET = "3fb6648662e40e9bda1d12c0b1e98236e7a7974630450a02a9a3c82c50dea15545d52cb84e6ec3fe8ef6d4d7894735766c8dd26ec6a6955d6ed541120cfaab9c"
CHART_PERIOD = 300
CHECK_PERIOD = 300
# how much percentage of the money to market buy/sell with
MARKET_BUY_PERCENTAGE = 0.0
# whether to print wealth during the training phase
PRINT_WEALTH = False
# whether to print weights during the training phase
PRINT_WEIGHTS = False
# do not buy if is in simulation
IS_SIMULATION = False
# WRITE FILE
IS_WRITE_TO_DATA = True
IS_WRITE_LOG = True


async def main():
    global p
    global CHECK_PERIOD
    global LAST_CHECK_DATE
    global LATEST_DATE
    global CHART_PERIOD
    global weights
    global ratioStrat
    try:
        while True:
            if time() - LAST_CHECK_DATE > CHECK_PERIOD:
                print("Checking for new data... LATEST DATE: {}".format(LATEST_DATE))
                tickers = p.returnTicker()
                tickersWithDate = {
                    t: float(tickers[t]["last"]) for t in tickers if t in allPairs}
                NEW_DATE = LATEST_DATE + CHECK_PERIOD
                print("NEW DATE: {}".format(NEW_DATE))
                tickersWithDate["date"] = NEW_DATE
                # update LATEST_DATE
                LATEST_DATE = NEW_DATE
                newData = pd.DataFrame(tickersWithDate, index=[0])
                # rearrange the columns for the new data to fit the old
                newData = newData[data.columns]
                if IS_WRITE_TO_DATA:
                    with open("./poloTestData.csv", "a") as f:
                        newData.to_csv(f, header=False)
                ratioStrat.updateData(newData)
                # print(ratioStrat.getRatios())
                ratio = ratioStrat.getLatestRatio()
                # print(ratio)

                if ratio is not None:
                    weights = pamr.step(ratio, weights, update_wealth=True)
                    wealthString = "Theoretical percentage increase: {}".format(
                        pamr.wealth)
                    if IS_WRITE_LOG:
                        with open("./wealthText3.txt", "a") as f:
                            f.write(wealthString + "\n")
                    weightsString = "New weights: {}".format(weights)
                    if IS_WRITE_LOG:
                        with open("./weightsText3.txt", "a") as f:
                            f.write(weightsString + "\n")
                    print(weightsString)
                    print(wealthString)
                    pairsWeights = pairsToWeights(allPairs, weights)
                    if not IS_SIMULATION:
                        portfolio.sendEvent(ReconfigureEvent(pairsWeights))

            LAST_CHECK_DATE = time()

            # print("Data update loop sleeping for {} seconds...".format(CHECK_PERIOD))
            await asyncio.sleep(CHECK_PERIOD)
    except asyncio.CancelledError:
        # LOG
        log.debug(
            ">> Coroutine: main cancelled")

if __name__ == "__main__":
    p = poloniex.Poloniex(API_KEY, SECRET)

    allPairs = [a for a in p.returnTicker().keys() if a.startswith("BTC")
                and not a.endswith("GNO")]
    print("Pairs are:")
    print(allPairs)

    print("Downloading data from Poloniex")
    # Downloads new data from Poloniex
    firstPair = False
    if True:
        for pair in allPairs:
            if not firstPair:
                data = DataFrame.from_dict(p.returnChartData(
                    pair, CHECK_PERIOD, time() - 2 * p.MONTH)).ix[:, ['date', 'close']]
                data.columns = ['date', pair]
                firstPair = True
            else:
                newData = DataFrame.from_dict(p.returnChartData(
                    pair, CHECK_PERIOD, time() - 2 * p.MONTH)).ix[:, ['date', 'close']]
                newData.columns = ['date', pair]
                data = data.join(newData.set_index('date'), on="date")

        data.to_csv("poloTestData3.csv")
    # GET LATEST DATE
    data = pd.read_csv("./poloTestData3.csv", index_col=0)
    # print(data)
    print(data)
    # get latest date for reference
    LATEST_DATE = data["date"].iloc[-1]

    # to sync the data by syncing the last check date with the latest date
    LAST_CHECK_DATE = LATEST_DATE

    # remove NANs in the data
    data = cleanNANs(data)
    ratioStrat = ratioStrategy.BasicRatioStrategy(
        data, dataPeriod=CHECK_PERIOD, chartPeriod=CHART_PERIOD)
    # run the pamr algo
    pamr = PAMR(ratios=ratioStrat.getRatios())
    weights = pamr.train(update_wealth=PRINT_WEALTH,
                         print_weights=PRINT_WEIGHTS)
    # print(weights)
    pairsWeights = pairsToWeights(allPairs, weights)
    if not IS_SIMULATION:
        portfolio = Portfolio(p, initialPairsWeights=pairsWeights,
                              marketBuyPercentage=MARKET_BUY_PERCENTAGE)

    loop = asyncio.get_event_loop()
    loop.create_task(main())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(portfolio.stop())
        loop.close()
