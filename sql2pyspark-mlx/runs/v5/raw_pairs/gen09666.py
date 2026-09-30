from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") > "2024-07-11")
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("region") == "North", F.col("sqft")).otherwise(0)).alias("north_sqft"),
        F.avg("sqft").alias("avg_sqft"),
    )
)
