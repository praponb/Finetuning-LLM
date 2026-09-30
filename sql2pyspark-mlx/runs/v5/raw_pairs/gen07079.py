from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0)).alias("umbrella_count"),
    )
    .orderBy(F.col("last_launch_date").desc())
)
