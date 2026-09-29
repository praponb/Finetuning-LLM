from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 3, 6, 7]) | F.col("sale_date").between("2023-07-01", "2023-08-01"))
    .groupBy(
        F.when(F.col("units") >= 32, "high").when(F.col("units") >= 8, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.avg("units").alias("avg_units"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
    )
    .orderBy(F.col("avg_units").desc())
)
