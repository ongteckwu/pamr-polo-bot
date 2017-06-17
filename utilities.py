import numpy as np


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
        cleanNANcolumn(data[col], dataLength - 1)

    return data
