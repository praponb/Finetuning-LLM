from pyspark.sql import functions as F

valid_stores = df_stores.filter(F.col("sqft") == 8481)

summary = (
    valid_stores
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.sum(F.when(F.col("region") == "North", 1).otherwise(0)).alias("north_count"),
    )
)

result = (
    summary
    .select("opened_month", "min_sqft", "north_count")
    .orderBy(F.col("north_count").desc(), "opened_month")
)
