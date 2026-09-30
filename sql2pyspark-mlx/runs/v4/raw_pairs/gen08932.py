from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0)).alias("umbrella_count"),
    )
    .orderBy("category", F.col("umbrella_count").desc())
)
