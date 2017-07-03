import poloniex
import pandas as pd
import logging
import asyncio
import sys
import ratioStrategy
import logger
import configparser

from pprint import pprint
from time import time, sleep
from pandas import DataFrame
from PAMR import PAMR
from utilities import cleanNANs, pairsToWeights
from Portfolio import Portfolio, ReconfigureEvent

config = configparser.ConfigParser()
config.read("./config.ini")
LOG = logger.Logger(config["LOGGING"])
# sections should have TRADING, DATA, and LOGGING

# GET ALL PAIRS
API_KEY = config["TRADING"]["API_KEY"]
SECRET = config["TRADING"]["SECRET"]
CHART_PERIOD = int(config["DATA"]["CHART_PERIOD"])
CHECK_PERIOD = int(config["DATA"]["CHECK_PERIOD"])
# how much percentage of the money to market buy/sell with
MARKET_BUY_PERCENTAGE = float(config["TRADING"]["MARKET_BUY_PERCENTAGE"])
DOWNLOAD_DATA = config["DATA"].getboolean("DOWNLOAD_DATA")
IS_WRITE_DATA = config["DATA"].getboolean("WRITE_NEW_DATA")
DATA_FILE = config["DATA"]["DATA_FILE"]
IS_SIMULATION = config["TRADING"].getboolean("IS_SIMULATION")
SIMULATION_AMOUNT = float(config["TRADING"]["SIMULATION_AMOUNT"])


async def main():
    global p
    global CHECK_PERIOD
    global LAST_CHECK_DATE
    global LATEST_DATE
    global CHART_PERIOD
    global WRITE_DATA_STREAM
    global weights
    global ratioStrat
    try:
        while True:
            if time() - LAST_CHECK_DATE > CHECK_PERIOD:
                print("Checking for new data... LATEST DATE: {}".format(LATEST_DATE))
                tickers = p.returnTicker()
                tickersWithDate = {
                    t: ((float(tickers[t]["lowestAsk"]) +
                         float(tickers[t]["highestBid"])) / 2) for t
                    in tickers if t in allPairs}
                NEW_DATE = LATEST_DATE + CHECK_PERIOD
                print("NEW DATE: {}".format(NEW_DATE))
                tickersWithDate["date"] = NEW_DATE
                # update LATEST_DATE
                LATEST_DATE = NEW_DATE
                newData = pd.DataFrame(tickersWithDate, index=[0])
                # rearrange the columns for the new data to fit the old
                newData = newData[data.columns]
                if IS_WRITE_DATA:
                    newData.to_csv(WRITE_DATA_STREAM, header=False)
                ratioStrat.updateData(newData)
                # print(ratioStrat.getRatios())
                ratio = ratioStrat.getLatestRatio()
                # print(ratio)

                if ratio is not None:
                    weights = pamr.step(ratio, weights, update_wealth=True)
                    print("Theoretical percentage increase: {}".format(
                        pamr.wealth))
                    if LOG["LOG_WEALTH"]:
                        if LOG.checkFileEmpty("LOG_WEALTH"):
                            LOG.writeToFile("wealth, date")
                        LOG.writeToFile("LOG_WEALTH", str(
                            pamr.wealth) + "," + str(time))
                    print("New weights: {}".format(weights))
                    if LOG["LOG_WEIGHTS"]:
                        if LOG.checkFileEmpty("LOG_WEIGHTS"):
                            LOG.writeToFile(
                                "LOG_WEIGHTS", weights.to_csv(header=True))
                        else:
                            LOG.writeToFile(
                                "LOG_WEIGHTS", weights.to_csv(header=False))
                    pairsWeights = pairsToWeights(allPairs, weights)
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
    if DOWNLOAD_DATA:
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

        # HACKISH
        data.to_csv(DATA_FILE)
    # GET LATEST DATE
    data = pd.read_csv(DATA_FILE, index_col=0)
    # if write new data
    if IS_WRITE_DATA:
        WRITE_DATA_STREAM = open(DATA_FILE, 'a')

    # get latest date for reference
    LATEST_DATE = data["date"].iloc[-1]

    # to sync the data by syncing the last check date with the latest date
    LAST_CHECK_DATE = LATEST_DATE
    # remove NANs in the data
    data = cleanNANs(data)
    print(data)
    ratioStrat = ratioStrategy.BasicRatioStrategy(
        data, dataPeriod=CHECK_PERIOD, chartPeriod=CHART_PERIOD)
    # run the pamr algo
    pamr = PAMR(ratios=ratioStrat.getRatios())
    weights = pamr.train()
    print(weights)
    pairsWeights = pairsToWeights(allPairs, weights)
    portfolio = Portfolio(p, initialPairsWeights=pairsWeights,
                          marketBuyPercentage=MARKET_BUY_PERCENTAGE,
                          logger=LOG,
                          isSimulation=IS_SIMULATION,
                          simulationAmount=SIMULATION_AMOUNT)

    loop = asyncio.get_event_loop()
    loop.create_task(main())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(portfolio.stop())
        loop.close()
