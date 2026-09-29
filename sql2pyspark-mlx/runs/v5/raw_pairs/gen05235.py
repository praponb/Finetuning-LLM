from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(~((F.col("sale_date") <= "2024-04-01") & (F.col("units") <= 35)))
    .withColumn("revenue_adjusted", F.round((F.col("revenue") * 1.1), 2))
)

agg = (
    base
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("revenue_adjusted"), 2).alias("stddev_revenue_adjusted"),
        F.count("revenue_adjusted").alias("revenue_adjusted_count"),
    )
)

result = (
    agg
    .filter(F.col("revenue_adjusted_count") >= 7)
    .select("product_id", "stddev_revenue_adjusted", "revenue_adjusted_count")
)
