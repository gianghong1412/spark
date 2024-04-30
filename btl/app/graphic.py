import csv
from matplotlib import pyplot
from collections import Counter
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sn
import plotly.express as px

if __name__ == "__main__":
        # dt = pd.read_csv('out/totalPriceSale/totalPriceSale.csv')
        # plt.figure(figsize=(16,8))
        # plt.barh(dt["year"],dt["total_price"],color="red")
        # plt.xlabel("Tong doanh thu")
        # plt.ylabel("Nam")
        # plt.title("Doanh thu ban hang cac nam")
        # plt.show()

        # dt = pd.read_csv('out/totalSalePerMonthInYear/totalSalePerMonthInYear.csv')
        # plt.figure(figsize=(28,8))
        # plt.plot(dt["timeData"],dt["total_price"],color="green",marker="o",linestyle="solid")
        # plt.ylabel("Doanh thu $",fontsize=6)
        # plt.xlabel("Thoi gian",fontsize=6)
        # plt.xticks(fontsize=6,rotation=90)
        # plt.legend(loc=(1.1,.5))
        # plt.grid(linestyle="--")
        # plt.show()

        # dt = pd.read_csv('out/genderCount/genderCount.csv')
        # plt.figure(figsize=(18,18))
        # plt.pie(dt["count"], \
        #         labels=dt["gender"], \
        #         autopct="%1.2f%%", \
        #         colors=["#FFB6D9","#99DBF5"])
        # plt.title("Ti le khach hang nam va nu")
        # plt.legend()
        # plt.show()

        # dt = pd.read_csv('out/aveAgeGender/aveAgeGender.csv')
        # plt.figure(figsize=(4,10))
        # plt.bar(dt["gender"],dt["age_averange"],color="red")
        # plt.xlabel("Gioi tinh")
        # plt.ylabel("Do tuoi trung binh")
        # plt.grid(linestyle="--")
        # plt.title("Do tuoi trung binh khach hang nam va nu")
        # plt.show()

        # dt = pd.read_csv('out/priceByGender/priceByGender.csv')
        # plt.figure(figsize=(4,10))
        # plt.bar(dt["gender"],dt["total_price"],color="red")
        # plt.xlabel("Gioi tinh")
        # plt.ylabel("Chi phi mua sam")
        # plt.grid(linestyle="--")
        # plt.title("Chi phi mua sam cua khach hang nam va nu")
        # plt.show()

        # dt = pd.read_csv('out/ageCount/ageCount.csv')
        # plt.figure(figsize=(16,8))
        # plt.barh(dt["age"],dt["count"],color="red")
        # plt.xlabel("So luong")
        # plt.ylabel("Tuoi")
        # plt.yticks(dt["age"],fontsize=5)
        # plt.grid(linestyle="--")
        # plt.title("So luong khach hang theo tuoi")
        # plt.show()       

        # dt = pd.read_csv('out/quantityCategoryByGender/quantityCategoryByGender.csv')
        # sn.barplot(data=dt,x=dt["category"],y=dt["total_quantity"],hue=dt["gender"],palette=["red","blue"])
        # plt.xlabel("Loai mat hang")
        # plt.ylabel("So luong dat hang")
        # plt.title("So luong mua hang theo gioi tinh va loai mat hang")
        # plt.xticks(dt["category"],fontsize=10,rotation=45)
        # plt.show() 

        # dt = pd.read_csv('out/priceCategoryByGender/priceCategoryByGender.csv')
        # sn.barplot(data=dt,x=dt["category"],y=dt["total_price"],hue=dt["gender"],palette=["red","blue"])
        # plt.xlabel("Loai mat hang")
        # plt.ylabel("Ton doanh thu")
        # plt.title("Doanh thu mua hang theo gioi tinh va loai mat hang")
        # plt.xticks(dt["category"],fontsize=10,rotation=45)
        # plt.show() 



        # dt = pd.read_csv('out/totalPriceMarket/totalPriceMarket.csv')
        # plt.figure(figsize=(16,8))
        # plt.bar(dt["shopping_mall"],dt["total_price"],color="red")
        # plt.xlabel("Doanh thu")
        # plt.ylabel("Mall")
        # plt.yticks(fontsize=5,rotation=45)
        # plt.xticks(dt['shopping_mall'],fontsize=8,rotation=45)
        # plt.grid(linestyle="--")
        # plt.title("Doanh thu theo shopping mall")
        # plt.show() 


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

        dt = pd.read_csv('out/totalPriceCategory/totalPriceCategory.csv')
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