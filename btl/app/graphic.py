import csv
from matplotlib import pyplot
from collections import Counter
import pandas as pd
import matplotlib.pyplot as plt

if __name__ == "__main__":
    df = pd.read_csv('out/totalPriceSale.csv/part-00000-10ba2a99-85b0-4134-8fd4-fd029dde5cc5-c000.csv')
    df.plot.barh(x='year',y='total_price',color='blue')
    plt.show()   


    dt = pd.read_csv('out/genderCount/part-00000-a0b496b0-9328-4368-b0ab-a6ba29673b99-c000.csv')
    plt.pie(dt["count"], \
            labels=dt["gender"], \
            autopct="%1.2f%%", \
            colors=["#FFB6D9","#99DBF5"])
    plt.title("Male vs Female")
    plt.show()

    
