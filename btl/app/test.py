from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as f

if __name__ == "__main__":
    session = SparkSession.builder.appName("test").getOrCreate()
    dataFrameReader = session.read

    data_Schema = StructType() \
        .add("invoice_no",StringType(),True) \
        .add("customer_id",StringType(),True) \
        .add("gender",StringType(),True) \
        .add("age",IntegerType(),True) \
        .add("category",StringType(),True) \
        .add("quantity",IntegerType(),True) \
        .add("price",FloatType(),True) \
        .add("payment_method",StringType(),True) \
        .add("invoice_date",StringType(),True) \
        .add("shopping_mall",StringType(),True)
    # finalStruct = StructType(data_Schema)
    dataSale = dataFrameReader \
        .option("header","true") \
        .schema(data_Schema) \
        .csv("in/customer_shopping_data.csv") 

    print ("====Print schema===")
    dataSale.printSchema()

    dataSale.describe().show()

    print ("===Print selected columns")
    dataSale.withColumn("invoice_date",to_date(col("invoice_date"),"dd/mm/yyyy"))
    dataSale = dataSale.select("invoice_no","customer_id","gender","age","category","quantity","price","payment_method","invoice_date","shopping_mall")

    splitDate = split(dataSale["invoice_date"],"/")
    dataSale = dataSale.withColumn("month",splitDate.getItem(1).cast(IntegerType())) \
                        .withColumn("year",splitDate.getItem(2))
    dataSale = dataSale.distinct()

    #analysis and visualization
    # print("tong doanh thu theo nam")
    # totalPriceSale = dataSale \
    #     .groupBy("year") \
    #     .agg(sum("price").alias("total_price")) \
    #     .orderBy("year", ascending = False) \
    #     .write.mode("overwrite") \
    #     .option("header", "true") \
    #     .csv("out/totalPriceSale")
    
    # print("===doanh thu trung binh moi thang ===")
    # totalSalePerMonth = dataSale \
    #     .groupBy("month") \
    #     .agg(sum("price").alias("total_price")) \
    #     .orderBy("month", ascending = False)
    # aveSalePerMonth = totalSalePerMonth.agg(avg("total_price").alias("average of month")).show()

    # print("===doanh thu moi thang===")

    # totalSalePerMonthInYear = dataSale \
    #     .groupBy("year","month") \
    #     .agg(round(sum("price"),2).alias("total_price")) \
    #     .orderBy("year","month", ascending = True) \
    #     .withColumn("timeData",concat_ws("/",col("month"),col("year"))) \
    #     .write.mode("overwrite") \
    #     .option("header", "true") \
    #     .csv("out/totalSalePerMonthInYear")

    # print("====ti le khach hang nam va nu===")
    # genderCount = dataSale \
    #     .groupBy("gender") \
    #     .agg(count("gender").alias("count")) \
    #     .write.mode("overwrite") \
    #     .option("header", "true") \
    #     .csv("out/genderCount")
    
    # print("=== do tuoi trung binh theo gioi tinh===")
    # aveAgeGender = dataSale \
    #     .groupBy("gender") \
    #     .agg(avg("age").alias("age_averange")) \
    #     .write.mode("overwrite") \
    #     .option("header", "true") \
    #     .csv("out/aveAgeGender")
    
    # print("=== do tuoi khach hang===")
    # ageCount = dataSale \
    #     .groupBy("age") \
    #     .count() \
    #     .orderBy("age") \
    #     .write.mode("overwrite") \
    #     .option("header", "true") \
    #     .csv("out/ageCount")
       
    # print ("===so luong loai mat hang theo gioi tinh")
    # quantityCategoryByGender = dataSale \
    #     .groupBy("gender","category") \
    #     .agg(sum("quantity").alias("total_quantity")) \
    #     .orderBy("category","gender") \
    #     .write.mode("overwrite") \
    #     .option("header", "true") \
    #     .csv("out/quantityCategoryByGender")
    
    print ("===doanh thu theo gioi tinh")
    priceByGender = dataSale \
        .groupBy("gender") \
        .agg(sum("price").alias("total_price")) \
        .write.mode("overwrite") \
        .option("header", "true") \
        .csv("out/priceByGender")

    # print ("===doanh thu cac loai mat hang theo gioi tinh")
    # priceCategoryByGender = dataSale \
    #     .groupBy("gender","category") \
    #     .agg(sum("price").alias("total_price")) \
    #     .orderBy("category","gender") \
    #     .write.mode("overwrite") \
    #     .option("header", "true") \
    #     .csv("out/priceCategoryByGender")
    
    # print("===Doanh thu cua cac dia diem===")
    # totalPriceMarket = dataSale \
    #     .groupBy("shopping_mall") \
    #     .agg(round(sum("price"),2).alias("total_price")) \
    #     .orderBy("total_price", ascending = False) \
    #     .write.mode("overwrite") \
    #     .option("header", "true") \
    #     .csv("out/totalPriceMarket")

    # print ("===Doanh thu cua tung mat hang===")
    # totalPriceCategory = dataSale \
    #     .groupBy("category") \
    #     .agg(sum("price").alias("total_price"),sum("quantity").alias("total_quantity")) \
    #     .orderBy("total_price", ascending = False)
    # totalPriceCategory.write.mode("overwrite") \
    #                         .option("header", "true") \
    #                         .csv("out/totalPriceCategory")
    # print ("===Doanh thu 20 don hang tot nhat ====")
    # topPrice  = dataSale \
    #     .orderBy("price",ascending = False) \
    #     .show(100)

    # print("=== so luong don hang theo so luong hang===")
    # countInvoiceByQuantity = dataSale \
    #     .groupBy("quantity") \
    #     .count() \
    #     .show()

    print("===Doanh thu theo hinh thuc thanh toan")
    priceMenthod = dataSale \
        .groupBy("payment_method") \
        .agg(round(sum("price"),2).alias("total_price")) \
        .orderBy("total_price",ascending = False) \
        .show()
    
    print("===So luong theo invoice_no")
    quantityInvoiceNo = dataSale \
        .groupBy("invoice_no") \
        .count().alias("count") \
        .filter(col("count")>1) \
        .orderBy("count",ascending = False) \
        .show()
    
    
    # print ("===doanh thu tung loai hang theo thang trong 2022")
    # totalCategoryEachMonIn2022 = dataSale.filter(dataSale['year'] == "2022") \
    #                 .groupBy("month","category") \
    #                 .agg(sum("price").alias("total_price")) \
    #                 .orderBy("month",ascending = False)
                       
    # totalCategoryEachMonIn2022.write.mode("overwrite") \
    #                         .option("header", "true") \
    #                         .csv("out/totalCategoryEachMonIn2022.csv")
    
    # print ("===doanh thu tung loai hang theo thang trong 2021")
    # totalCategoryEachMonIn2022 = dataSale.filter(dataSale['year'] == "2021") \
    #                 .groupBy("month","category") \
    #                 .agg(sum("price").alias("total_price")) \
    #                 .orderBy("month",ascending = False)
                       
    # totalCategoryEachMonIn2022.write.mode("overwrite") \
    #                         .option("header", "true") \
    #                         .csv("out/totalCategoryEachMonIn2021.csv")
                    
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
