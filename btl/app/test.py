from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, split
from pyspark.sql.types import IntegerType

def getMonth(word: str):
    month = word
    if length(month) < 2 : month = "0" + month
    return month

if __name__ == "__main__":
    session = SparkSession.builder.appName("test").getOrCreate()
    dataFrameReader = session.read

    response = dataFrameReader \
        .option("header","true") \
        .option("inferSchema", value = True )\
        .csv("btl/in/customer_shopping_data.csv")

    # print ("====Print schema===")
    # response.printSchema()

    print ("===Print selected columns")
    responSelectedColumns = response.select("category","quantity","price","invoice_date")
    responSelectedColumns.show(100)

    # print ("===total quantity of category")
    # responSelectedColumns \
    #     .groupBy("Category") \
    #     .agg(sum("quantity").alias("total_quantity")) \
    #     .orderBy("total_quantity", ascending = False).show()
    
    # print ("===add date month year columns===")
    splitDate = split(responSelectedColumns["invoice_date"],"/")
    responseSplitDate = responSelectedColumns.withColumn("day",splitDate.getItem(0)) \
                                            .withColumn("month",splitDate.getItem(1).cast(IntegerType())) \
                                            .withColumn("year",splitDate.getItem(2))
    # responseSplitDate.show(truncate=False)

    # print ("===add total_price each year")
    responseSplitDate \
        .groupBy("year") \
        .agg(sum("price").alias("total_price")) \
        .orderBy("total_price", ascending = False)

    print ("===total price each category in 2022")
    response2022 = responseSplitDate.filter(responseSplitDate['year'] == "2022") \
                    .groupBy("month") \
                    .agg(sum("price").alias("total_price")) \
                    .orderBy("total_price",ascending = False) \
                    
    
    print ("===total price each category in 2021")
    response2021 = responseSplitDate.filter(responseSplitDate['year'] == "2021") \
                    .groupBy("month") \
                    .agg(sum("price").alias("total_price")) \
                    .orderBy("total_price",ascending = False) \
                    
    response2021 \
        .write.mode("overwrite") \
        .option("header", "true") \
        .csv("btl/out/2021.csv")
    
    response2022 \
        .write \
        .option("header", "true") \
        .csv("btl/out/2022.csv")

    print ("=======================")
session.stop()

