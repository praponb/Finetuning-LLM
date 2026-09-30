from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") != "South")
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.max("opened_date").alias("last_opened_date"),
        F.count("sqft").alias("sqft_count"),
    )
    .filter(F.col("sqft_count") > 3)
)
