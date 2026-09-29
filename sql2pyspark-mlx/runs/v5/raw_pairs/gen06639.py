from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter(F.col("region").isin(["Central", "North", "South"]) & (F.col("city") != "Tokyo"))
)

summary = (
    filtered_stores
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "East", 1).otherwise(0))) / F.count("*")), 1).alias("pct_east"),
    )
)

result = summary.select("opened_month", "pct_east")
