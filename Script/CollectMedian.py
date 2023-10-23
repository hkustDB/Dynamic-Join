import getopt
import math
import sys
import numpy as np
import time
import os
import re
import statistics as stat


Ours_list_ER = [0.092, 0.0065, 0.172, 0.254, 0.119, 0.34, 0.19,\
                1.99, 0.145, 3.7, 7.6, 3.61, 29.7, 3.74,\
                9.6, 1.37, 4.41, 7.63, 1.14, 30.1, 0.318,\
                0.279, 0.00302, 0.377, 0.554, 0.263, 2.16, 0.337,\
                1.7, 0.0262, 1.89, 2.83, 1.15, 12.6, 1.36]

BM_list_ER = [56.8, 3.08, 2.09, 0.882, 3.13, 15.4, 15.1,\
              6.26e5, 6.06e4, 6.03e3, 1060, 9.95e3, 1.6e5, 1.03e4,\
             2.12e3, 272, 12.2, 4.6, 5.04, 375, 1.21,\
            2.73e5, 2.61e3, 1.21e4, 210,1.45e3, 5.43e4, 1.38e3,\
            3.92e9, 4.12e7, 1.4e6, 1.32e5, 3.55e6, 2.11e8, 2.16e6]

CM_list_ER = [1.28, 0.107, 3.54, 14.8, 1.95, 2.21, 2.89,\
              1.89e3, 212, 54.5, 52.4, 69.5, 1.37e3, 63.2,\
             73.3, 10.7, 11.3, 23.9, 3.71, 32, 5.18,\
             1.17e3, 11.6, 33.1, 34.3, 33, 149, 34.8,\
            2.57e6, 2.46e4, 1.07e3, 104, 2.56e3, 9.43e4, 2.31e3]

RS_list_ER = [5.82e4, 4.25e4, 9.32e3, 4.84e3, 1.19e4, 3.79e4, 1.2e4,\
              1.84e9, 9.42e9, 1.49e8, 2.73e7, 1.63e8, 1.81e9, 1.75e8,\
                2.17e11, 1.37e12, 6.37e9, 3.19e9, 3.14e9, 9.03e10, 8.57e8,\
                1.21e9, 3.63e8, 2.34e7, 4.85e6, 3.4e7, 3.64e8, 3.34e7,\
                8.05e13, 1.03e14, 3.56e11, 2.03e10, 7.31e11, 9.25e12, 3.92e11]

BM_quotients = [a/b for a, b in zip(BM_list_ER, Ours_list_ER)]
# print(BM_quotients)
BM_quotients = sorted(BM_quotients)
BM_median = stat.median(BM_quotients)

CM_quotients = [a/b for a, b in zip(CM_list_ER, Ours_list_ER)]
CM_quotients = sorted(CM_quotients)
CM_median = stat.median(CM_quotients)

RS_quotients = [a/b for a, b in zip(RS_list_ER, Ours_list_ER)]
RS_quotients = sorted(RS_quotients)
RS_median = stat.median(RS_quotients)

path = '../Experiment/Stat/stat_median.txt'
stat_file = open(path, 'w')

BM_writeline = "BM_quotients: %s\nBM_median: %s\n"%(BM_quotients, BM_median)
CM_writeline = "CM_quotients: %s\nCM_median: %s\n"%(CM_quotients, CM_median)
RS_writeline = "RS_quotients: %s\nRS_median: %s\n"%(RS_quotients, RS_median)

stat_file.write(BM_writeline)
stat_file.write(CM_writeline)
stat_file.write(RS_writeline)

stat_file.close()