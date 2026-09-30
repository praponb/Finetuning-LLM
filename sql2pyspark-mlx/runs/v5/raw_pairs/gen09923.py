from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("region").isin(["West", "North", "South"]) & (F.col("city") != "Bangkok"))
    .withColumn("clean_store_name", F.trim("store_name"))
)

opened_month_summary = (
    base
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("sqft")).otherwise(0)).alias("singapore_sqft"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("sqft")).otherwise(0)).alias("tokyo_sqft"),
    )
)

result = (
    opened_month_summary
    .filter(F.col("tokyo_sqft") < 45)
    .select("opened_month", "singapore_sqft", "tokyo_sqft")
    .orderBy(F.col("tokyo_sqft").desc())
)
