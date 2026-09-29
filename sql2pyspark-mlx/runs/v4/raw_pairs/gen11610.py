from pyspark.sql import functions as F

filtered_products = (
    df_products
    .filter(F.col("price") <= 750)
    .withColumn("stock_qty_minus_price", (F.col("stock_qty") - F.col("price")))
)

agg = (
    filtered_products
    .groupBy("category")
    .agg(
        F.sum(F.when(F.col("brand") == "Initech", 1).otherwise(0)).alias("initech_count"),
    )
)

result = agg.select("category", "initech_count")
