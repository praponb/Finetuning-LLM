from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") >= "2025-04-01")
    .groupBy("product_id")
    .agg(
        F.max("units").alias("max_units"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
    .orderBy("stddev_units", "product_id")
    .limit(50)
)
