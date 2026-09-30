from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["West", "East"]) | (F.col("opened_date") >= "2024-04-08"))
    .groupBy("region", F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
    .filter(F.col("sqft_range") < 74)
    .orderBy(F.col("sqft_range").desc(), "region", "opened_month")
    .limit(10)
)
