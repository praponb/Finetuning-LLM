from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
        F.max("opened_date").alias("latest_opened_date"),
    )
    .filter(F.col("new_york_count") < 8)
    .select("region", "new_york_count", "latest_opened_date")
)
