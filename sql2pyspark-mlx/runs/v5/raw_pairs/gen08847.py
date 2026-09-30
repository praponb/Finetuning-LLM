from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") < 40)
    .groupBy(
        "product_id",
        "store_id",
        F.when(F.col("units") >= 36, "high").when(F.col("units") >= 8, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg((F.max("units") - F.min("units")).alias("units_range"))
    .orderBy("units_bucket")
)
