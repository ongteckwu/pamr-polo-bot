import poloniex
import pprint
import os
import pandas as pd
import logging

from time import time, sleep
from pandas import DataFrame
from PAMR import PAMR
from utilities import cleanNANs

# GET ALL PAIRS
p = poloniex.Poloniex()
CHECK_PERIOD = p.MINUTE  # check every minute
LAST_CHECK_DATE = time() - CHECK_PERIOD
LATEST_DATE = None
CHART_PERIOD = 14400
LAST_TRADE_PERIOD = None
INITIAL_AMOUNT = 1.00

allPairs = [a for a in p.returnTicker().keys() if a.startswith("BTC")
            and not a.endswith("GNO")]
print("Pairs are:")
print(allPairs)

# COMBINE PAIRS INTO ONE DATAFRAME
print("Downloading data from Poloniex")
firstPair = False
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


def pairsToWeights(pairs, weights):
    pairsWeights = {}
    for i in range(len(pairs)):
        pairsWeights[pairs[i]] = weights[i]
    return pairsWeights


# removes index and data column and removes NANs
data = cleanNANs(data)
cleanedData = data.drop(data.columns[[0, 1]], 1)
pamr = PAMR(data=cleanedData)
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
                    newDataTemp = DataFrame.from_dict(cdata).ix[:, ['date', 'close']]
                    newDataTemp.columns = ['date', pair]
                    newData = newData.join(newDataTemp.set_index('date'), on="date")

            if hasAllNewData:
                # update data
                data.append(newData)
                # update weights
                ratios = data.iloc[-1][2:] / data.iloc[-2][2:]
                weights = pamr.step(ratios, weights, update_wealth=True)
                print("New weights: {}".format(weights))
                pairsWeights = pairsToWeights(allPairs, weights)
                portfolio.sendEvent(ReconfigureEvent(pairsWeights))
                # update LATEST_DATE
                LATEST_DATE = nDate

        LAST_CHECK_DATE = time()

        print("Sleeping for {} seconds...".format(CHECK_PERIOD))
        sleep(CHECK_PERIOD)

except KeyboardInterrupt:
    print('bye')
    os._exit(0)