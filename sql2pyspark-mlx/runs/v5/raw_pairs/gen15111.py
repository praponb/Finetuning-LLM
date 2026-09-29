from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").isNotNull())
    .groupBy("store_id")
    .agg(
        F.max("units").alias("max_units"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
    .filter(F.col("avg_units") > 29)
)
