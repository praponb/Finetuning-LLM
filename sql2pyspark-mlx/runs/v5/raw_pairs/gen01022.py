from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(~F.col("status").isin(["EXPIRED", "PAUSED", "CANCELLED"]) & F.col("customer_id").isin([27, 33]))
)

agg = (
    src
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(((100 * F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0))) / F.count("*")), 1).alias("pct_premium"),
    )
)

result = (
    agg
    .filter(F.col("pct_premium") >= 62)
    .select("status", "status_list", "pct_premium")
)
