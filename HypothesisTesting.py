from scipy.stats import normaltest


class HypothesisTesting:
    @staticmethod
    def normality_test(x_test):
        return normaltest(x_test)

