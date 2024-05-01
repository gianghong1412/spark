from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
from datetime import datetime

if __name__ == "__main__":
    session = SparkSession.builder.appName("btl").getOrCreate()
    dataFrameReader = session.read
    data_Schema = StructType() \
        .add("InvoiceNo",StringType(),True) \
        .add("StockCode",StringType(),True) \
        .add("Description",StringType(),True) \
        .add("Quantity",IntegerType(),True) \
        .add("InvoiceDate",DateType(),True) \
        .add("UnitPrice",FloatType(),True) \
        .add("CustomerID",StringType(),True) \
        .add("Country",StringType(),True) 
        
    dataSale = dataFrameReader \
        .option("header","true") \
        .option('inferschema',True) \
        .csv("in/OnlineRetail.csv") 
    print ("====Print schema===")
    dataSale.printSchema()
    dataSale.describe().show()
    dataSale.na.drop()
    dataSale =  dataSale \
        .filter(dataSale['Quantity']>0) \
        .filter(dataSale['UnitPrice']>0)

    # splitDate = split(dataSale["InvoiceDate"],"/")
    # dataSale = dataSale.withColumn("month",splitDate.getItem(0).cast(IntegerType())) \
    #                     .withColumn("year",splitDate.getItem(2))

    # print("tong doanh thu theo nam")
    # totalPriceSale = dataSale \
    #     .groupBy("year") \
    #     .agg(round(sum(dataSale['UnitPrice']*dataSale["Quantity"]),2).alias("total_price")) \
    #     .orderBy("year", ascending = False) \
    #     .write.mode("overwrite") \
    #     .option("header", "true") \
    #     .csv("outBTL/totalPriceSale")
    
    # print("===doanh thu moi thang===")
    # totalSalePerMonthInYear = dataSale \
    #     .groupBy("year","month") \
    #     .agg(round(sum(dataSale['UnitPrice']*dataSale["Quantity"]),2).alias("total_price")) \
    #     .orderBy("year","month", ascending = True) \
    #     .withColumn("timeData",concat_ws("/",col("month"),col("year"))) \
    #     .write.mode("overwrite") \
    #     .option("header", "true") \
    #     .csv("outBTL/totalSalePerMonthInYear")

    # print("===Doanh thu cua cac nuoc===")
    # totalPriceMarket = dataSale \
    #     .groupBy("Country") \
    #     .agg(round(sum(dataSale['UnitPrice']*dataSale["Quantity"]),2).alias("total_price")) \
    #     .orderBy("total_price", ascending = False) \
    #     .write.mode("overwrite") \
    #     .option("header", "true") \
    #     .csv("outBTL/totalPriceMarket")

    print ("===Doanh thu cua tung mat hang===")
    totalPriceCategory = dataSale \
        .groupBy("StockCode") \
        .agg(round(sum(dataSale['UnitPrice']*dataSale["Quantity"]),2).alias("total_price")) \
        .orderBy("total_price", ascending = False) \
        .write.mode("overwrite") \
        .option("header", "true") \
        .csv("outBTL/totalPriceCategory")

    dataCustomerCount = dataSale \
        .groupBy("CustomerID") \
        .agg(count_distinct("InvoiceNo").alias('frequency')) \
        .write.mode("overwrite") \
        .option("header", "true") \
        .csv("outBTL/dataCustomerCount")
    
    # dataCustomerAmount = dataSale \
    #     .groupBy("CustomerID") \
    #     .agg(round(sum(dataSale['UnitPrice']*dataSale["Quantity"]),2).alias("total_price")) \
    #     .write.mode("overwrite") \
    #     .option("header", "true") \
    #     .csv("outBTL/dataCustomerAmount")

    # dataCustomerRecency = dataSale \
    #     .select("CustomerID","InvoiceDate") \
    #     .write.mode("overwrite") \
    #     .option("header", "true") \
    #     .csv("outBTL/dataCustomerRecency")
    
        

