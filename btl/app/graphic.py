import csv
from matplotlib import pyplot
from collections import Counter
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sn
import plotly.express as px
import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from kmodes.kprototypes import KPrototypes

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

        # dt = pd.read_csv('out/totalPriceCategory/totalPriceCategory.csv')
        # plt.figure(figsize=(16,8))
        # plt.bar(dt["category"],dt["total_price"],color="red")
        # plt.xlabel("category")
        # plt.ylabel("total_price")
        # plt.title("Doanh thu ban hang cua tung loai mat hang")
        # plt.show()

        # plt.figure(figsize=(8,8))
        # plt.pie(dt["total_price"],labels=dt["category"],autopct="%1.2f%%")
        # plt.title("ti le doanh thu giua cac loai mat hang")
        # plt.legend(loc=(1.1,.5))
        # plt.show()
    dt = pd.read_csv('in/customer_shopping_data.csv')
    dt_new = dt.drop(['invoice_no','customer_id','category','gender','payment_method','invoice_date','shopping_mall'],axis=1,inplace=True)
    dt_new = pd.DataFrame(dt_new)
    # dt_new.info()
    # sn.pairplot(dt.iloc[:,2:])
    # scaled_dt = dt.copy()
    # columnToNomalize = ['age','quantity','price']
    # scaled_dt[columnToNomalize] = scaled_dt[columnToNomalize].apply(lambda x: (x-x.mean())/np.std(x))
    # scaled_dt = scaled_dt.iloc[:,1:]

    # cost = []
    # for num_clusters in list(range(1,14)):
    #     kproto = KPrototypes(n_clusters=num_clusters,init ='huang')
    #     kproto.fit_predict(scaled_dt,categorical=[0])
    #     cost.append(kproto.cost_)
    # plt.plot(cost)
    # plt.xlabel('Number of cluster')

    # kmean = KMeans(n_clusters=4,max_iter=50,n_init=10)
    # kmean.fit(dt)
    # kmean.labels_
    # ssd = []
    # range_n_clusters = [2,3,4,5,6,7,8]
    # for num_cluster in range_n_clusters:
    #     kmean = KMeans(n_clusters=num_cluster,max_iter=50,n_init=10)
    #     kmean.fit(dt)
    #     ssd.append(kmean.inertia_)
    #     print("For n_cluster={0}, the Elbow score is {1}".format(num_cluster,kmean.inertia_))

    # fig = px.line(x=range_n_clusters,y =ssd,
    #               title="Elbow cuver for K-mean Clustering",
    #               labels={'x':'Number of Cluster','y':'Sum of Square Distances SSD'})
    # fig.update_layout(
    #     xaxis=dict(title_font=dict(size=14)),
    #     yaxis=dict(title_font=dict(size=14)),
    #     showlegend=False,
    #     width=800,
    #     height=600
    # )
    # fig.show()
        # silhouette_avg = silhouette_score(dt,kmean.labels_)
        # print("For n_cluster={0}, the silhouette is {1}".format(num_cluster,silhouette_avg))
    kmean = KMeans(n_clusters=3,max_iter=50,n_init=10)
    kmean.fit(dt)
    dt["cluter_id"] = kmean.labels_
    dt.info()

    fig = px.box(dt,x='cluter_id',y='price',title="Cluster Id and price",
                 labels={'cluter_id': 'Cum','price':'Chi tieu'},
                 color='cluter_id')
    fig.update_layout(
        xaxis=dict(title="Cum",title_font=dict(size=14)),
        yaxis=dict(title="Chi tieu",title_font=dict(size=14)),
        showlegend=False,
        width=800,
        height=600)
    fig.show()

    # fig = px.box(dt,x='cluter_id',y='age',title="Cluster Id and price",
    #              labels={'cluter_id': 'Cum','price':'Tuoi'},
    #              color='cluter_id')
    # fig.update_layout(
    #     xaxis=dict(title="Cum",title_font=dict(size=14)),
    #     yaxis=dict(title="Chi tieu",title_font=dict(size=14)),
    #     showlegend=False,
    #     width=800,
    #     height=600)
    # fig.show()

    # fig = px.box(dt,x='cluter_id',y='quantity',title="Cluster Id and price",
    #              labels={'cluter_id': 'Cum','price':'So luong dat hang'},
    #              color='cluter_id')
    # fig.update_layout(
    #     xaxis=dict(title="Cum",title_font=dict(size=14)),
    #     yaxis=dict(title="Chi tieu",title_font=dict(size=14)),
    #     showlegend=False,
    #     width=800,
    #     height=600)
    # fig.show()

    fig = px.scatter(dt,x='cluter_id',y='price', color='quantity',title="Cluster Id and price",
                 labels={'cluter_id': 'Cum','price':'Chi tieu','quantity':'Soluong'}
                 )
    fig.update_layout(
        xaxis=dict(title="Cum",title_font=dict(size=14)),
        yaxis=dict(title="Chi tieu",title_font=dict(size=14)),
        showlegend=False,
        width=800,
        height=600)
    fig.show()



    print("=============================")

