from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 20) & (F.col("monthly_fee") > 130))
    .groupBy("customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_expired"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"),
    )
    .orderBy(F.col("customer_id").desc(), F.col("active_count").desc())
)
