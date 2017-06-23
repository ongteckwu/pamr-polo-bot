import poloniex
import pandas as pd
import logging
import asyncio
import sys
import ratioStrategy

from time import time, sleep
from pandas import DataFrame
from PAMR import PAMR
from utilities import cleanNANs, pairsToWeights
from Portfolio import Portfolio, ReconfigureEvent


log = logging.getLogger(__name__)
logPortfolio = logging.getLogger(Portfolio.__name__)
handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.DEBUG)
log.addHandler(handler)
logPortfolio.addHandler(handler)

# GET ALL PAIRS
API_KEY = "8N0DP32H-UDZX8K7F-Q70NU866-BD501GE0"
SECRET = "3fb6648662e40e9bda1d12c0b1e98236e7a7974630450a02a9a3c82c50dea15545d52cb84e6ec3fe8ef6d4d7894735766c8dd26ec6a6955d6ed541120cfaab9c"
CHECK_PERIOD = 60
LAST_CHECK_DATE = time() - CHECK_PERIOD
LATEST_DATE = None
CHART_PERIOD = 7200
MARKET_BUY_PERCENTAGE = 0.1


async def main():
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
                # changed to False if not all data is present
                hasAllNewData = True
                newData = None
                # to update latest date
                nDate = None
                for pair in allPairs:
                    cdata = p.returnChartData(pair, CHART_PERIOD, LATEST_DATE+1)
                    # if new data is found, the following will not be run
                    if (len(cdata) == 0) or cdata[0]["date"] <= 0:
                        # no new data
                        hasAllNewData = False
                        break
                    else:
                        for d in cdata:
                            if d["date"] > LATEST_DATE:
                                if ((nDate is None) or d["date"] > nDate):
                                    nDate = d["date"]

                    # if there's data (won't reach here if no data)
                    if newData is None:
                        newData = DataFrame.from_dict(
                            cdata).ix[:, ['date', 'close']]
                        newData.columns = ['date', pair]
                    else:
                        newDataTemp = DataFrame.from_dict(
                            cdata).ix[:, ['date', 'close']]
                        newDataTemp.columns = ['date', pair]
                        newData = newData.join(
                            newDataTemp.set_index('date'), on="date")

                if hasAllNewData:
                    # check for NANs
                    noOfNANs = newData.isnull().sum().sum()
                    print("Number of nans: {}".format(noOfNANs))
                    if not (noOfNANs > len(allPairs) * 0.1 and noOfNANs > 3):
                        oldData = ratioStrat.getData().iloc[-1]
                        newData = newData.fillna(oldData)
                        logging.debug("New data arrived")
                        # update data
                        # data.append(newData)
                        # ratios = data.iloc[-1][2:] / data.iloc[-2][2:]
                        # update weights
                        ratios = ratioStrat.updateDataAndRatio(newData)
                        weights = pamr.step(
                            ratios, weights, update_wealth=True)
                        print("New weights: {}".format(weights))
                        print("Theoretical percentage increase: {}".format(
                            pamr.wealth))
                        pairsWeights = pairsToWeights(allPairs, weights)
                        portfolio.sendEvent(ReconfigureEvent(pairsWeights))
                        # update LATEST_DATE
                        LATEST_DATE = nDate
                    else:
                        print("Number of nans: {} - SKIP UPDATE".format(noOfNANs))

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

    # COMBINE PAIRS INTO ONE DATAFRAME
    print("Downloading data from Poloniex")
    firstPair = False
    if True:
        for pair in allPairs:
            if not firstPair:
                data = DataFrame.from_dict(p.returnChartData(
                    pair, CHART_PERIOD, time() - 2 * p.MONTH)).ix[:, ['date', 'close']]
                data.columns = ['date', pair]
                firstPair = True
            else:
                newData = DataFrame.from_dict(p.returnChartData(
                    pair, CHART_PERIOD, time() - 2 * p.MONTH)).ix[:, ['date', 'close']]
                newData.columns = ['date', pair]
                data = data.join(newData.set_index('date'), on="date")

        data.to_csv("poloTestData.csv")
    # GET LATEST DATE
    data = pd.read_csv("./poloTestData.csv")
    LATEST_DATE = data["date"].iloc[-1]

    data = cleanNANs(data)
    ratioStrat = ratioStrategy.BasicRatioStrategy(data)
    pamr = PAMR(ratios=ratioStrat.getRatios())
    weights = pamr.train()
    print(weights)
    pairsWeights = pairsToWeights(allPairs, weights)
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
