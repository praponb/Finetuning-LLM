from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-06-03", "2025-09-01"))
    .groupBy(
        F.when(F.col("units") >= 36, "high").when(F.col("units") >= 15, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(F.avg("revenue").alias("avg_revenue"))
    .filter(F.col("avg_revenue") >= 2300)
)
