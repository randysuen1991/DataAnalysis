import numpy as np
import pandas as pd


class DataAnalysis:

    # This method points out the data which are outliers, determined by the standard deviation in the group of data.
    # input argument should be the data itself and the number which is going to be used to judge whether a datum is
    # a outlier or not.
    @staticmethod
    def outlierhandler(x_train, num):
        std = np.std(x_train)
        mean = np.mean(x_train)
        centered_x_train = x_train - mean
        deviation = centered_x_train / std
        index = np.where(deviation > num)
        return index
