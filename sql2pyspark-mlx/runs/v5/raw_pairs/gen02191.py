from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.month("start_date").isin([3, 4, 10]) | (F.col("start_date") >= "2023-11-01"))
)

summary = (
    base
    .groupBy("status", F.year("start_date").alias("start_year"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "basic", 1).otherwise(0))) / F.count("*")), 1).alias("pct_basic"),
    )
)

result = (
    summary
    .filter(F.col("pct_basic") < 64)
    .select("status", "pct_basic")
)
