import csv
from matplotlib import pyplot
from collections import Counter
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sn
import plotly.express as px
import numpy as np
import datetime as dt
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler, LabelEncoder

if __name__ == "__main__":
    dataCustomerAmount = pd.read_csv('outBTL/dataCustomerAmount/dataCustomerAmount.csv')
    dataCustomerRecency = pd.read_csv('outBTL/dataCustomerRecency/dataCustomerRecency.csv')
    dataCustomerCount = pd.read_csv('outBTL/dataCustomerCount/dataCustomerCount.csv')
    dataCustomerAC = dataCustomerAmount.merge(dataCustomerCount,on='CustomerID')
    dataCustomerRecency['InvoiceDate'] = pd.to_datetime(dataCustomerRecency['InvoiceDate'], format='mixed')
    dataCustomerRecency['diff'] = max(dataCustomerRecency['InvoiceDate']) - dataCustomerRecency['InvoiceDate']
    dataCustomerRecency = dataCustomerRecency.groupby('CustomerID')['diff'].min().reset_index()
    dataCustomerRecency['diff'] = dataCustomerRecency['diff'].dt.days
    dataCustomerACR = dataCustomerAC.merge(dataCustomerRecency,on='CustomerID')
    dataCustomerACR.rename(columns={'diff' : 'recency'}, inplace=True)
    print(dataCustomerACR)

    attributes = ['total_price', 'frequency', 'recency']
    data = dataCustomerACR[attributes]

    # Create a box plot with Plotly
    # fig = px.box(data, y=attributes, labels={'variable': 'Attributes', 'value': 'Range'},
    #             title="Outliers Variable Distribution")
    # fig.update_layout(
    #     xaxis=dict(title="Attributes"),
    #     yaxis=dict(title="Range"),
    #     showlegend=False,
    #     boxmode='group',  # Display box plots side by side
    #     width=800, height=600
    # )
    # fig.show()


    dataACR = dataCustomerACR[['total_price', 'frequency', 'recency']]

    # Instantiate
    scaler = StandardScaler()

    # fit_transform
    dataACRScale = scaler.fit_transform(dataACR)
    ssd = []
    range_n_clusters = [2, 3, 4, 5, 6, 7, 8]

    for num_clusters in range_n_clusters:
        kmeans = KMeans(n_clusters=num_clusters, max_iter=50, n_init=10)
        kmeans.fit(dataACRScale)
        
        ssd.append(kmeans.inertia_)
        print("For n_clusters={0}, the Elbow score is {1}".format(num_clusters, kmeans.inertia_))

    fig = px.line(x=range_n_clusters, y=ssd, 
                    title="Elbow Curve for K-Means Clustering",
                    labels={'x': 'Number of Clusters', 'y': 'Sum of Squared Distances (SSD)'})

    fig.update_layout(
        xaxis=dict(title_font=dict(size=14)),
        yaxis=dict(title_font=dict(size=14)),
        showlegend=False,
        width=800,
        height=600
    )

    fig.show()

    kmeans = KMeans(n_clusters=3, max_iter=50)
    kmeans.fit(dataACRScale)
    kmeans.labels_
    dataCustomerACR['ClusterNum'] = kmeans.labels_
    print(dataCustomerACR)

    fig = px.box(dataCustomerACR, x='ClusterNum', y='total_price',
             title="Nhom va tong chi tieu Box Plot",
             labels={'ClusterNum': 'Nhom', 'total_price': 'Tong chi tieu'},
             color='ClusterNum')

    fig.update_layout(
        xaxis=dict(title="Nhom", title_font=dict(size=14)),
        yaxis=dict(title="Tong chi tieu", title_font=dict(size=14)),
        showlegend=False,
        width=800,
        height=600
    )
    fig.show()

    fig = px.box(dataCustomerACR, x='ClusterNum', y='frequency',
             title="Nhom va so lan mua hang Box Plot",
             labels={'ClusterNum': 'Nhom', 'frequency': 'So lan mua'},
             color='ClusterNum')

    fig.update_layout(
        xaxis=dict(title="Nhom", title_font=dict(size=14)),
        yaxis=dict(title="So lan mua", title_font=dict(size=14)),
        showlegend=False,
        width=800,
        height=600
    )
    fig.show()

    fig = px.box(dataCustomerACR, x='ClusterNum', y='recency',
             title="Nhom va khoang cach mua hang Box Plot",
             labels={'ClusterNum': 'Nhom', 'recency': 'Khoang cach mua'},
             color='ClusterNum')

    fig.update_layout(
        xaxis=dict(title="Nhom", title_font=dict(size=14)),
        yaxis=dict(title="Khoang cach mua", title_font=dict(size=14)),
        showlegend=False,
        width=800,
        height=600
    )
    fig.show()

    fig = px.scatter(dataCustomerACR, x='ClusterNum', y='total_price', color='frequency',
                 title='Nhom va tong chi tieu (Mau sac the hien lan mua hang)',
                 labels={'ClusterNum': 'Nhom', 'total_price': 'Tong chi tieu', 'frequency': 'Frequency'})

    fig.update_layout(
        xaxis=dict(title="Nhom", title_font=dict(size=14)),
        yaxis=dict(title="Tong chi tieu", title_font=dict(size=14)),
        showlegend=True,
        width=800,
        height=600
    )
    fig.show()

    custom_palette = sn.color_palette(["#FF0000", "#00FF00", "#0000FF"])
    # Create a scatter plot matrix with separate plots for each cluster, custom palette, and a larger size
    sn.set(style="ticks")
    sn.pairplot(dataCustomerACR, hue='ClusterNum', vars=['total_price', 'frequency'], palette=custom_palette, height=4, aspect=1.5)
    plt.suptitle('Nhom va tong chi tieu', y=1.02)
    plt.show()

    # Selecting the two features for clustering
    data_for_clustering = dataCustomerACR[['total_price', 'frequency']]
    # Apply K-Means clustering
    kmeans = KMeans(n_clusters=3)
    dataCustomerACR['Cluster_2D'] = kmeans.fit_predict(data_for_clustering)

    # Create a scatter plot with Plotly
    fig = px.scatter(dataCustomerACR, x='total_price', y='frequency', color='Cluster_2D',
                    title='Moi quan he giua nhom, chi tieu va tan suat',
                    labels={'total_price': 'Tong chi tieu', 'srequency': 'So lan mua hang', 'Cluster_2D': 'Nhom'})

    fig.update_layout(
        xaxis=dict(title="Tong chi tieu", title_font=dict(size=14)),
        yaxis=dict(title="So lan mua hang", title_font=dict(size=14)),
        width=800,
        height=600
    )

    fig.show()

    # Selecting the three features for clustering
    data_for_clustering = dataCustomerACR[['total_price', 'frequency', 'recency']]

    # Apply K-Means clustering
    kmeans = KMeans(n_clusters=3)
    dataCustomerACR['Cluster_3D'] = kmeans.fit_predict(data_for_clustering)

    # Visualize the clusters (scatter plot matrix)
    sn.set(style="ticks")
    sn.pairplot(dataCustomerACR, hue='Cluster_3D', vars=['total_price', 'frequency', 'recency'], palette='Set1')
    plt.suptitle('Moi quan he giua nhom, tong chi tieu, tan suat va khoang cach mua hang', y=1.02)
    plt.show()






    