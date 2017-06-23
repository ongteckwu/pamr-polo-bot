import pandas as pd

from abc import ABC, abstractmethod
from threading import RLock


class RatioStrategy(ABC):
    def __init__(self, data):
        self.ratiosLock = RLock()
        self.dataLock = RLock()
        self.data = data[1:]  # data includes an index and a date
        self.ratios = None
        self.buildRatios()

    def getData(self):
        with self.dataLock:
            return self.data

    def getRatios(self):
        with self.ratiosLock:
            return self.ratios

    @abstractmethod
    def buildRatios(self):
        pass

    @abstractmethod
    def updateDataAndRatio(self, row):
        pass


class BasicRatioStrategy(RatioStrategy):
    def __init__(self, data):
        super().__init__(data)

    def buildRatios(self):
        with self.ratiosLock:
            cleanedData = self.data.drop(self.data.columns[[0, 1]], 1)
            self.ratios = (cleanedData / cleanedData.shift(1))[1:]

    def updateDataAndRatio(self, row):
        with self.dataLock:
            self.data.append(row)
            with self.ratiosLock:
                ratio = self.data.iloc[-1][2:] / self.data.iloc[-2][2:]
                self.ratios.append(ratio, ignore_index=True)
        return ratio


# class StaggeredRatioStrategy(RatioStrategy):
#     def __init__(self, data, staggerPeriod):
#         self.staggerPeriod = staggerPeriod
#         self.latestStaggerDate = None
#         self.latestStaggerBase = None
#         super().__init__(data)

#     def buildRatios(self):
#         with self.ratiosLock:
#             # cleanedData = self.data.drop(self.data.columns[[0, 1]], 1)
#             self.ratios, self.latestStaggerBase, self.latestStaggerDate = StaggeredRatioStrategy.staggeredRatios(
#                 self.data, self.staggerPeriod)

#     def updateDataAndRatio(self, rows):
#         with self.dataLock:
#             self.data.append(rows, ignore_index=True)
#             with self.ratiosLock:
#                 cleanedRows = rows.drop(rows.columns[[0]], 1).apply(pd.to_numeric)
#                 print(len(cleanedRows.columns))
#                 print(cleanedRows)
#                 print(self.latestStaggerBase)
#                 ratio = cleanedRows.div(self.latestStaggerBase)
#                 self.ratios.append(ratio, ignore_index=True)
#                 latestDate = rows[["date"]].iloc[-1][0]
#                 if (self.latestStaggerDate + self.staggerPeriod) >= latestDate:
#                     self.latestStaggerDate = latestDate
#                     self.latestStaggerBase = rows.iloc[-1]
#         print("RATIOS")
#         print(self.ratios.iloc[-1])
#         return self.ratios.iloc[-1]

#     @staticmethod
#     def staggeredRatios(data, chartPeriod):
#         # returns ratios based on last chart period
#         # so for instance if the latest date is 140000 and
#         # chart_period is 14400,
#         # all data from 140000 to 125600 will be ratioed by the data
#         # in 125600
#         latestDate = data[["date"]].iloc[-1][0]
#         earliestDate = data[["date"]].iloc[0][0]
#         ratios = pd.DataFrame()
#         test = 0
#         while (latestDate - earliestDate) >= chartPeriod:
#             currentDate = latestDate - chartPeriod
#             dataSubset = data.loc[(currentDate < data["date"])
#                                   & (data["date"] <= latestDate)]
#             dataSub = dataSubset.drop(dataSubset.columns[[0, 1]], 1)
#             dataBase = data.loc[data["date"] == currentDate].drop(
#                 data.columns[[0, 1]], 1).iloc[0, :]
#             # print(dataSub)
#             # print(type(dataBase))
#             ratiosSubset = dataSub.div(dataBase)
#             # if (test <= 3):
#             #     print(dataSub)
#             #     test+=1

#             ratios = ratios.append(ratiosSubset)
#             latestDate = currentDate
#         ratios = ratios.sort_index()
#         return ratios, dataBase, latestDate

if __name__ == "__main__":
    # Test BasicRatioStrategy
    data = pd.read_csv("./poloTestData.csv")
    brs = BasicRatioStrategy(data)
    # print(brs.ratios)