from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 44, "high").when(F.col("units") >= 14, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(F.max("units").alias("max_units"))
    .filter(F.col("max_units") > 11)
    .select("units_bucket", "max_units")
)
