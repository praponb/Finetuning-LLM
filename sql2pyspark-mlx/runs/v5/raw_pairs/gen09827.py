from pyspark.sql import functions as F

recent_store_sales = (
    df_store_sales
    .filter((F.col("units") > 32) | F.col("sale_date").between("2023-03-30", "2024-09-01"))
)

summary = (
    recent_store_sales
    .groupBy(
        F.when(F.col("revenue") >= 3500, "high").when(F.col("revenue") >= 2000, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(F.count("units").alias("units_count"))
)

result = (
    summary
    .filter(F.col("units_count") < 7)
    .select("revenue_bucket", "units_count")
    .orderBy(F.col("units_count").desc(), "revenue_bucket")
    .limit(5)
)
