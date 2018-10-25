import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller as adf
from sklearn.covariance import EllipticEnvelope


class DataAnalysis:
    # This method points out the data which are outliers, determined by the standard deviation in the group of data.
    # input argument should be the data itself and the number which is going to be used to judge whether a datum is
    # a outlier or not.
    @staticmethod
    def outlierremoving(x_train, num_of_std, axis=0):
        std = np.std(x_train, axis=axis)
        mean = np.mean(x_train, axis=axis)
        centered_x_train = x_train - mean
        deviation = centered_x_train / std
        index = np.where(np.abs(deviation) > num_of_std)
        return index

    @staticmethod
    def gaussian_outlier_detection(x_train):
        pass


class TimeSeriesAnalysis:
    # Augmented Dickey-Fuller test
    @staticmethod
    def adfuller(x_train):
        return adf(x_train)

