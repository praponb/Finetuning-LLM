from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") > 3300)
    .groupBy("product_id", F.quarter("sale_date").alias("sale_quarter"), "store_id")
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") < 4)
    .orderBy("avg_units")
)
