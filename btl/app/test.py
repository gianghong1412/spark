from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import seaborn as sns

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

    #total number of sales each years
    # totalNumberSale = dataSale \
    #     .groupBy("year") \
    #     .agg(sum("price").alias("total_price")) \
    #     .orderBy("total_price", ascending = False)
    
    #average sale per month, monthly revenue
    
    # aveSalePerMonth2021 = dataSale \
    #     .filter(dataSale["year"]=="2021") \
    #     .groupBy("month") \
    #     .agg(avg("price").alias("average"), \
    #          sum("price").alias("price")) \
    #     .orderBy("average", ascending = False)
    # aveSalePerMonth2021.show()

    # aveSalePerMonth2022 = dataSale \
    #     .filter(dataSale["year"]=="2022") \
    #     .groupBy("month") \
    #     .agg(avg("price").alias("average"), \
    #          sum("price").alias("price")) \
    #     .orderBy("average", ascending = False)
    # aveSalePerMonth2022.show()

    # key demographic of customer
    genderCount = dataSale \
        .groupBy("gender") \
        .agg(count("gender").alias("countg")) \
        .show()
    plt.figure(figsize=(8,8))
    plt.pie(genderCount["countg"], \
            labels=genderCount["gender"], \
            autopct="%1.2f%%", \
            colors=["#FFB6D9","#99DBF5"])
    plt.title("Male vs Female")
    plt.legend()
    plt.show()
    
    aveAgeGender = dataSale \
        .groupBy("gender") \
        .agg(avg("age").alias("age averange")) \
        .show()

    # responSelectedColumns \
    #     .groupBy("Category") \
    #     .agg(sum("quantity").alias("total_quantity")) \
    #     .orderBy("total_quantity", ascending = False).show()
    
    # print ("===add date month year columns===")
    # splitDate = split(responSelectedColumns["invoice_date"],"/")
    # responseSplitDate = responSelectedColumns.withColumn("day",splitDate.getItem(0)) \
                                            # .withColumn("month",splitDate.getItem(1).cast(IntegerType())) \
                                            # .withColumn("year",splitDate.getItem(2))
    # responseSplitDate.show(truncate=False)

    # print ("===add total_price each year")
    # responseSplitDate \
    #     .groupBy("year") \
    #     .agg(sum("price").alias("total_price")) \
    #     .orderBy("total_price", ascending = False)

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
