import csv
from matplotlib import pyplot
from collections import Counter
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sn

if __name__ == "__main__":
        # dt = pd.read_csv('out/totalPriceSale/part-00000-de780aca-7381-463d-afa9-d137331512da-c000.csv')
        # plt.figure(figsize=(16,8))
        # plt.barh(dt["year"],dt["total_price"],color="red")
        # plt.xlabel("Tong doanh thu")
        # plt.ylabel("Nam")
        # plt.title("Doanh thu ban hang cac nam")
        # plt.show()

        # dt = pd.read_csv('out/totalSalePerMonthInYear/part-00000-b3d62014-6b2d-4660-92b9-49e1cd9613c5-c000.csv')
        # plt.figure(figsize=(28,8))
        # plt.plot(dt["timeData"],dt["total_price"],color="green",marker="o",linestyle="solid")
        # plt.ylabel("Doanh thu $",fontsize=6)
        # plt.xlabel("Thoi gian",fontsize=6)
        # plt.xticks(fontsize=6,rotation=90)
        # plt.legend(loc=(1.1,.5))
        # plt.grid(linestyle="--")
        # plt.show()

        dt = pd.read_csv('out/genderCount/part-00000-a9f2c6f0-c27e-4242-81ed-fab568cff1cc-c000.csv')
        plt.figure(figsize=(18,18))
        plt.pie(dt["count"], \
                labels=dt["gender"], \
                autopct="%1.2f%%", \
                colors=["#FFB6D9","#99DBF5"])
        plt.title("Ti le khach hang nam va nu")
        plt.legend()
        plt.show()

    # dt = pd.read_csv('out/totalCategoryEachMonIn2022.csv/part-00000-363b2467-0645-48b8-9e1a-0e6346201b82-c000.csv')
    # sn.barplot(data=dt,x=dt["month"],y=dt["total_price"],hue=dt["category"],palette=["red","yellow","green","blue","purple","pink","black","grey"])
    # plt.xlabel("category")
    # plt.ylabel("total_price")
    # plt.title("Doanh thu loai hang hoa theo tung thang trong nam 2022")
    # plt.show()

    # dt = pd.read_csv('out/totalCategoryEachMonIn2021.csv/part-00000-363b2467-0645-48b8-9e1a-0e6346201b82-c000.csv')
    # sn.barplot(data=dt,x=dt["month"],y=dt["total_price"],hue=dt["category"],palette=["red","yellow","green","blue","purple","pink","black","grey"])
    # plt.xlabel("category")
    # plt.ylabel("total_price")
    # plt.title("Doanh thu loai hang hoa theo tung thang trong nam 2021")
    # plt.show()

        dt = pd.read_csv('out/totalPriceCategory/part-00000-229ac4fb-766c-4209-b4d0-ae01c4c08e64-c000.csv')
        plt.figure(figsize=(16,8))
        plt.bar(dt["category"],dt["total_price"],color="red")
        plt.xlabel("category")
        plt.ylabel("total_price")
        plt.title("Doanh thu ban hang cua tung loai mat hang")
        plt.show()

        plt.figure(figsize=(8,8))
        plt.pie(dt["total_price"],labels=dt["category"],autopct="%1.2f%%")
        plt.title("ti le doanh thu giua cac loai mat hang")
        plt.legend(loc=(1.1,.5))
        plt.show()