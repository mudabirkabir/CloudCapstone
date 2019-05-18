import powerlaw

import matplotlib.pyplot as plt

file = open('.\output\g3_1.log', 'r')

data = []

for line in file:
    count = line.split(", ")
    data.append(float(count[1].split(')')[0]))


fit = powerlaw.Fit(data)

dataplot = fit.plot_ccdf(color='r', linewidth=3,label='our data')

fit.power_law.plot_ccdf(ax=dataplot, color='b', linestyle='-', label='powerlaw fit')

fit.lognormal.plot_ccdf(ax=dataplot, color='g', linestyle='-', label='Log Normal fit')

plt.legend()

plt.show()