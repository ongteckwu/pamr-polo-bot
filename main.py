import poloniex
import pandas as pd
import logging
import asyncio

from time import time, sleep
from pandas import DataFrame
from PAMR import PAMR
from utilities import cleanNANs
from Portfolio import Portfolio, ReconfigureEvent

logging.basicConfig(level=logging.DEBUG)
# GET ALL PAIRS
API_KEY = "8N0DP32H-UDZX8K7F-Q70NU866-BD501GE0"
SECRET = "3fb6648662e40e9bda1d12c0b1e98236e7a7974630450a02a9a3c82c50dea15545d52cb84e6ec3fe8ef6d4d7894735766c8dd26ec6a6955d6ed541120cfaab9c"
CHECK_PERIOD = 60  # check every minute
LAST_CHECK_DATE = time() - CHECK_PERIOD
LATEST_DATE = None
CHART_PERIOD = 7200
INITIAL_AMOUNT = 0.03


def pairsToWeights(pairs, weights):
    pairsWeights = {}
    for i in range(len(pairs)):
        pairsWeights[pairs[i]] = weights[i]
    return pairsWeights


async def main():
    global CHECK_PERIOD
    global LAST_CHECK_DATE
    global LATEST_DATE
    global CHART_PERIOD
    global weights
    try:
        while True:
            if time() - LAST_CHECK_DATE > CHECK_PERIOD:
                print("Checking for new data...")
                hasAllNewData = True  # changed to False if not all data is present
                newData = None
                nDate = None
                for pair in allPairs:
                    cdata = p.returnChartData(pair, CHART_PERIOD, LATEST_DATE)
                    # if new data is found, the following will not be run
                    hasNewData = False
                    if (len(cdata) == 0):
                        # no new data
                        break
                    else:
                        # check whether got new data
                        for d in cdata:
                            if d["date"] > LATEST_DATE:
                                hasNewData = True
                                if ((nDate is None) or d["date"] > nDate):
                                    nDate = d["date"]
                        if not hasNewData:
                            hasAllNewData = False
                            break
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
                    # update data
                    data.append(newData)
                    ratios = data.iloc[-1][2:] / data.iloc[-2][2:]
                    # update weights
                    weights = pamr.step(ratios, weights, update_wealth=True)
                    print("New weights: {}".format(weights))
                    pairsWeights = pairsToWeights(allPairs, weights)
                    portfolio.sendEvent(ReconfigureEvent(pairsWeights))

                    # update LATEST_DATE
                    LATEST_DATE = nDate

            LAST_CHECK_DATE = time()

            print("Sleeping for {} seconds...".format(CHECK_PERIOD))
            await asyncio.sleep(CHECK_PERIOD)
    except asyncio.CancelledError:
        # LOG
        logging.debug(
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
    print(data)
    LATEST_DATE = data["date"].iloc[-1]

    # removes index and data column and removes NANs
    data = cleanNANs(data)
    cleanedData = data.drop(data.columns[[0, 1]], 1)
    pamr = PAMR(data=cleanedData)
    weights = pamr.train()
    print(weights)
    pairsWeights = pairsToWeights(allPairs, weights)
    portfolio = Portfolio(p, initialAmount=INITIAL_AMOUNT,
                          initialPairsWeights=pairsWeights)
    loop = asyncio.get_event_loop()

    loop.create_task(main())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(portfolio.stop())
        loop.close()
