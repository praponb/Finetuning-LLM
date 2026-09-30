from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.lower(F.trim("store_name")) == "siam flagship") | (F.col("region") == "North"))
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.sum(F.when(F.col("region") == "East", 1).otherwise(0)).alias("east_count"),
    )
    .orderBy(F.col("east_count").desc())
)
