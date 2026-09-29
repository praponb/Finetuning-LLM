from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") != 25)
    .groupBy(
        F.when(F.col("units") >= 45, "high").when(F.col("units") >= 13, "mid").otherwise("low").alias("units_bucket"),
        "store_id",
    )
    .agg(
        F.count("units").alias("units_count"),
        F.max("sale_date").alias("latest_sale_date"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .orderBy("stddev_revenue", F.col("units_bucket").desc(), "store_id")
    .limit(5)
)
