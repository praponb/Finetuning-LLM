from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.count("*").alias("total_count"),
        F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0)).alias("chiang_mai_count"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("sqft")).otherwise(0)).alias("berlin_sqft"),
    )
    .orderBy("opened_month", F.col("total_count").desc())
    .limit(20)
)
