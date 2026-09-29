from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter((F.col("region") != "Central") & F.col("city").isin(["London", "Bangkok"]))
    .withColumn("region_lower", F.lower("region"))
)

opened_month_summary = (
    recent_stores
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("region") == "Central", 1).otherwise(0)).alias("central_count"),
    )
)

result = (
    opened_month_summary
    .filter(F.col("central_count") > 37)
    .select("opened_month", "central_count")
    .orderBy("central_count", "opened_month")
    .limit(3)
)
