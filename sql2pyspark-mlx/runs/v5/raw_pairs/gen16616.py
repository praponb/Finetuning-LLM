from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 4]) & F.col("product_id").isin([7, 9]) & (F.col("sale_date") >= "2024-06-01"))
    .groupBy(
        "store_id",
        F.when(F.col("units") >= 35, "high").when(F.col("units") >= 9, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        (F.max("units") - F.min("units")).alias("units_range"),
        F.max("units").alias("max_units"),
    )
)
