from pyspark.sql import functions as F

valid_stores = (
    df_stores
    .filter(F.col("region") == "East")
    .withColumn("opened_date_plus_35d", F.date_add("opened_date", 35))
)

summary = (
    valid_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", F.col("sqft")).otherwise(0)).alias("tokyo_sqft"),
        F.count("*").alias("store_count"),
    )
)

result = (
    summary
    .filter(F.col("store_count") < 9)
    .orderBy(F.col("store_count").desc(), "tokyo_sqft")
)
