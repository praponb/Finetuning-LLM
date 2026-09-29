from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "North")
    .agg(
        F.count("*").alias("store_count"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
)
