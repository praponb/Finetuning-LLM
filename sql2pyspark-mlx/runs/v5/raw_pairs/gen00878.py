from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") <= "2023-06-01")
    .groupBy("region")
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.sum(F.when(F.col("region") == "South", F.col("sqft")).otherwise(0)).alias("south_sqft"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
    )
    .orderBy("region")
)
