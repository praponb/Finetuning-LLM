from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isNotNull() & F.lower("store_name").like("%own%") & (F.col("opened_date") >= "2025-07-01"))
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
    )
    .filter(F.col("pct_london") < 74)
    .orderBy(F.col("pct_london").desc(), F.col("city").desc())
)
