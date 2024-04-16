from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, split

if __name__ == "__main__":
    session = SparkSession.builder.appName("test").getOrCreate()
    dataFrameReader = session.read

    response = dataFrameReader \
        .option("header","true") \
        .option("inferSchema", value = True )\
        .csv("btl/in/customer_shopping_data.csv")

    print ("====Print schema===")
    response.printSchema()

    # print ("===Print selected columns")
    responSelectedColumns = response.select("category","quantity","price","invoice_date")
    # responSelectedColumns.show()

    # print ("===total quantity of category")
    # responSelectedColumns \
    #     .groupBy("Category") \
    #     .agg(sum("quantity").alias("total_quantity")) \
    #     .orderBy("total_quantity", ascending = False).show()
    
    print ("===add date month year columns===")
    splitDate = split(responSelectedColumns["invoice_date"],"/")
    responseSplitDate = responSelectedColumns.withColumn("day",splitDate.getItem(0)) \
                                            .withColumn("month",splitDate.getItem(1)) \
                                            .withColumn("year",splitDate.getItem(2))
    # responseSplitDate.show(truncate=False)

    # print ("===add total_price each year")
    # responseSplitDate \
    #     .groupBy("year") \
    #     .agg(sum("price").alias("total_price")) \
    #     .orderBy("total_price", ascending = False).show()

    print ("===total price each category in 2023")
    response2023 = responseSplitDate.filter(responseSplitDate['year'] == "2022") \
                    .groupBy("month") \
                    .agg(sum("price").alias("total_price")) \
                    .orderBy("total_price",ascending = False) \
                    .show(100)
session.stop()

