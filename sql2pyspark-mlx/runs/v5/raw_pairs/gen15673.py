from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.count("price").alias("price_count"),
    )
    .orderBy("brand", F.col("cost_range").desc())
)
