import numpy as np
import pandas as pd


def cleanNANs(data):
    # substitute nans at the bottom of data with last known value
    def cleanNANcolumn(column, index):
        assert(index < len(column))
        value = column.iloc[index]

        # print(column.iloc[index])
        if np.isnan(column.iloc[index]):
            if index == 0:
                column.iloc[index] = 0
                return 0
            value = cleanNANcolumn(column, index - 1)
            column.iloc[index] = value

        return value

    dataLength = len(data)
    for col in data:
        print(col)
        cleanNANcolumn(data[col], dataLength - 1)

    return data


def pairsToWeights(pairs, weights):
    pairsWeights = {}
    for i in range(len(pairs)):
        pairsWeights[pairs[i]] = weights[i]
    return pairsWeights


def staggeredRatios(data, chartPeriod):
    # returns ratios based on last chart period
    # so for instance if the latest date is 140000 and
    # chart_period is 14400,
    # all data from 140000 to 125600 will be ratioed by the data
    # in 125600
    latestDate = data["date"].iloc[-1]
    earliestDate = data["date"].iloc[0]
    ratios = pd.DataFrame()
    while (latestDate - earliestDate) >= chartPeriod:
        currentDate = latestDate - chartPeriod
        dataSubset = data.loc[(currentDate < data["date"]) & (data["date"] <= latestDate)]
        dates = dataSubset.ix[:, ["date"]]
        dataSub = dataSubset.ix[:, dataSubset.columns != "date"]
        dataBase = data.loc[data["date"] == currentDate].ix[:, dataSubset.columns != "date"].iloc[0,:]
        # print(dataSub)
        # print(type(dataBase))
        ratiosSubset = dataSub.div(dataBase)
        ratios = ratios.append(pd.concat([dates, ratiosSubset], axis=1), ignore_index=True)
        latestDate = currentDate
    ratios = ratios.sort_values("date")
    return ratios



if __name__ == "__main__":
    data = pd.read_csv("./poloTestData.csv")
    LATEST_DATE = data["date"].iloc[-1]

    # removes index and data column and removes NANs
    data = cleanNANs(data)
    print(staggeredRatios(data, 14400))
    data = data.drop(data.columns[[0, 1]], 1)

