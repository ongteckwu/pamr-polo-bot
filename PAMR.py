from threading import RLock
from modules.universal import tools
from pprint import pprint

import pandas as pd
import numpy as np


class PAMR(object):
    """ Passive aggressive mean reversion strategy for portfolio selection.
    There are three variants with different parameters, see original article
    for details.

    Reference:
        B. Li, P. Zhao, S. C.H. Hoi, and V. Gopalkrishnan.
        Pamr: Passive aggressive mean reversion strategy for portfolio selection, 2012.
        http://www.cais.ntu.edu.sg/~chhoi/paper_pdf/PAMR_ML_final.pdf
    """

    def __init__(self, ratios, fee=0.0025, eps=0.5, C=500, variant=0):
        """
        :param eps: Control parameter for variant 0. Must be >=0, recommended value is
                    between 0.5 and 1.
        :param C: Control parameter for variant 1 and 2. Recommended value is 500.
        :param variant: Variants 0, 1, 2 are available.
        :param fee: Fee for making a transaction. Can be a function.
        """
        # input check
        if eps < 0:
            raise ValueError('epsilon parameter must be >=0')

        if variant == 0:
            if eps is None:
                raise ValueError('eps parameter is required for variant 0')
        elif variant == 1 or variant == 2:
            if C is None:
                raise ValueError('C parameter is required for variant 1,2')
        else:
            raise ValueError('variant is a number from 0,1,2')

        self.eps = eps
        self.C = C
        self.variant = variant
        self.ratios = ratios
        self.wealth = 1
        self.fee = fee
        self.lock = RLock()

    def init_weights(self, m):
        with self.lock:
            return np.ones(m) / m

    def get_fee(self):
        if callable(self.fee):
            return self.fee()
        return self.fee

    def step(self, x, last_b, update_wealth=False):
        with self.lock:
            # calculate return prediction
            if update_wealth:
                self.wealth = self.wealth * \
                    x.dot(last_b) * (1 - self.get_fee())
                print("Wealth: {}".format(self.wealth))
            b = self.update(last_b, x, self.eps, self.C)
            # print(b)
        return b

    def update(self, b, x, eps, C):
        """ Update portfolio weights to satisfy constraint b * x <= eps
        and minimize distance to previous weights. """
        x_mean = np.mean(x)
        le = max(0., np.dot(b, x) - eps)

        if self.variant == 0:
            lam = le / np.linalg.norm(x - x_mean)**2
        elif self.variant == 1:
            lam = min(C, le / np.linalg.norm(x - x_mean)**2)
        elif self.variant == 2:
            lam = le / (np.linalg.norm(x - x_mean)**2 + 0.5 / C)

        # limit lambda to avoid numerical problems
        lam = min(100000, lam)

        # update portfolio
        b = b - lam * (x - x_mean)

        # project it onto simplex
        return tools.simplex_proj(b)

    def train(self, update_wealth=False, print_weights=False):
        with self.lock:
            b = self.init_weights(len(self.ratios.columns))
            for index, row in self.ratios.iterrows():
                b = self.step(row, b, update_wealth)
                if print_weights:
                    print(b)
            return b


if __name__ == "__main__":
    data = pd.read_csv("./poloTestData.csv")
    # print(data)
    from utilities import cleanNANs
    data = cleanNANs(data.drop(data.columns[[0, 1]], 1))
    pamr = PAMR(data)
    pamr.train(True)
