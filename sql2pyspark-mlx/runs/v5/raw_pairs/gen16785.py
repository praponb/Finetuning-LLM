from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") < 3000) | (F.col("units") >= 18))
    .agg(
        F.avg("units").alias("avg_units"),
        F.max("revenue").alias("max_revenue"),
    )
    .orderBy(F.col("avg_units").desc())
)
