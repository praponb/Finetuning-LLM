from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", F.month("opened_date").alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("region") == "North", F.col("sqft")).otherwise(0)).alias("north_sqft"),
        F.count(F.when(F.col("region") == "North", 1)).alias("north_count"),
    )
)
