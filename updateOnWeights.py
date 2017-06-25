import poloniex
import os
import pandas as pd
import logging
import ratioStrategy

from time import time, sleep
from pandas import DataFrame
from PAMR import PAMR
from utilities import cleanNANs, pairsToWeights


# GET ALL PAIRS
p = poloniex.Poloniex()
CHECK_PERIOD = p.MINUTE  # check every minute
LAST_CHECK_DATE = time() - CHECK_PERIOD
LATEST_DATE = None
CHART_PERIOD = 300
LAST_TRADE_PERIOD = None

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


# removes index and data column and removes NANs
data = cleanNANs(data)
ratioStrat = ratioStrategy.BasicRatioStrategy(data)
# data = data.drop(data.columns[[0, 1]], 1)
# ratios = (data / data.shift(1))[1:]
pamr = PAMR(ratios=ratioStrat.getRatios())
weights = pamr.train()
print(weights)
pairsWeights = pairsToWeights(allPairs, weights)


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
                if (len(cdata) == 0):
                    # no new data
                    hasAllNewData = False
                    break
                else:
                    # check whether got new data
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
                print(newData)
                noOfNANs = newData.isnull().sum().sum()
                if not (noOfNANs > len(allPairs) * 0.1 and noOfNANs > 3):
                    oldData = ratioStrat.getData().iloc[-1]
                    newData = newData.fillna(oldData)
                    with open("./poloTestData.csv", "a") as f:
                        newData.to_csv(f, header=False)
                    ratios = ratioStrat.updateDataAndRatio(newData)
                    weights = pamr.step(ratios, weights, update_wealth=True)
                    print("New weights: {}".format(weights))
                    print("Theoretical percentage increase: {}".format(pamr.wealth))
                    pairsWeights = pairsToWeights(allPairs, weights)
                    # update LATEST_DATE
                    LATEST_DATE = nDate
                else:
                    print("Number of nans: {} - SKIP UPDATE".format(noOfNANs))

        LAST_CHECK_DATE = time()

        print("Sleeping for {} seconds...".format(CHECK_PERIOD))
        sleep(CHECK_PERIOD)

except KeyboardInterrupt:
    print('bye')
    os._exit(0)
