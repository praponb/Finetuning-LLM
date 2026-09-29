from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") > 1400) & (F.col("store_id") == 2))
    .groupBy(
        F.quarter("sale_date").alias("sale_quarter"),
        F.when(F.col("revenue") >= 4000, "high").when(F.col("revenue") >= 1500, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.count("units").alias("units_count"),
    )
    .orderBy(F.col("revenue_bucket").desc())
)
