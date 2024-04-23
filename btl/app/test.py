from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
import pyspark.pandas as ps
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np


if __name__ == "__main__":
    session = SparkSession.builder.appName("test").getOrCreate()
    dataFrameReader = session.read
    data_Schema = [ \
        StructField("invoice_no",StringType(),True), \
        StructField("customer_id",StringType(),True), \
        StructField("age",IntegerType(),True), \
        StructField("category",StringType(),True), \
        StructField("quantity",IntegerType(),True), \
        StructField("price",DoubleType(),True), \
        StructField("payment_method",StringType(),True), \
        StructField("invoice_date",DateType(),True), \
        StructField("shopping_mall",StringType(),True) \
    ]
    finalStruct = StructType(fields=data_Schema)
    dataSale = dataFrameReader \
        .option("header","true") \
        .option("inferschema", True) \
        .csv("in/customer_shopping_data.csv")

    print ("====Print schema===")
    dataSale.printSchema()

    print ("===Print selected columns")
    dataSale = dataSale.select("customer_id","gender","age","category","quantity","price","payment_method","invoice_date","shopping_mall")
    splitDate = split(dataSale["invoice_date"],"/")
    dataSale = dataSale.withColumn("month",splitDate.getItem(1).cast(IntegerType())) \
                                            .withColumn("year",splitDate.getItem(2))

    #analysis and visualization
    print("tong doanh thu theo nam")
    totalPriceSale = dataSale \
        .groupBy("year") \
        .agg(sum("price").alias("total_price")) \
        .orderBy("year", ascending = False) \
        .write.mode("overwrite") \
        .option("header", "true") \
        .csv("out/totalPriceSale.csv")
    
    print("===doanh thu trung binh moi thang nam 2021===")
    aveSalePerMonth2021 = dataSale \
        .filter(dataSale["year"]=="2021") \
        .groupBy("month") \
        .agg(avg("price").alias("average"), \
             sum("price").alias("price")) \
        .orderBy("average", ascending = False)
    aveSalePerMonth2021.show()

    print("===doanh thu trung binh moi thang nam 2022===")
    aveSalePerMonth2022 = dataSale \
        .filter(dataSale["year"]=="2022") \
        .groupBy("month") \
        .agg(avg("price").alias("average"), \
             sum("price").alias("price")) \
        .orderBy("average", ascending = False)
    aveSalePerMonth2022.show()

    print("==== count gender===")
    genderCount = dataSale \
        .groupBy("gender") \
        .agg(count("gender").alias("countg")) \
        .show()
    
    # schemaGenderCount = ("gender string,countg interger")
    # dfGenderCount = SparkSession.createDataFrame(genderCount,schemaGenderCount)
    # dpFrame = dfGenderCount.toPandas()
    # plt.figure(figsize=(8,8))
    # plt.pie(dpFrame["countg"], \
    #         labels=dpFrame["gender"], \
    #         autopct="%1.2f%%", \
    #         colors=["#FFB6D9","#99DBF5"])
    # plt.title("Male vs Female")
    # plt.legend()
    # plt.show()
    
    print("=== do tuoi trung binh theo gioi tinh===")
    aveAgeGender = dataSale \
        .groupBy("gender") \
        .agg(avg("age").alias("age averange")) \
        .show()

    print("===Doanh thu cua cac dia diem===")
    totalPriceMarket = dataSale \
        .groupBy("shopping_mall") \
        .agg(sum("price").alias("total_price")) \
        .orderBy("total_price", ascending = False).show()

    print ("===Doanh thu cua tung mat hang===")
    totalPriceCategory = dataSale \
        .groupBy("category") \
        .agg(sum("price").alias("total_price"),sum("quantity").alias("total_quantity")) \
        .orderBy("total_price", ascending = False).show()
    
    print ("===Doanh thu hang nam===")
    totalYear=dataSale \
        .groupBy("year") \
        .agg(sum("price").alias("total_price")) \
        .orderBy("total_price", ascending = False) \
        .show()

    # print ("===total price each category in 2022")
    # response2022 = responseSplitDate.filter(responseSplitDate['year'] == "2022") \
    #                 .groupBy("month") \
    #                 .agg(sum("price").alias("total_price")) \
    #                 .orderBy("total_price",ascending = False) \
                    
    
    # print ("===total price each category in 2021")
    # response2021 = responseSplitDate.filter(responseSplitDate['year'] == "2021") \
    #                 .groupBy("month") \
    #                 .agg(sum("price").alias("total_price")) \
    #                 .orderBy("total_price",ascending = False) \
                    
    # response2021 \
    #     .write.mode("overwrite") \
    #     .option("header", "true") \
    #     .csv("out/2021.csv")
    
    # response2022 \
    #     .write \
    #     .option("header", "true") \
    #     .csv("out/2022.csv")

    print ("=======================")
session.stop()
