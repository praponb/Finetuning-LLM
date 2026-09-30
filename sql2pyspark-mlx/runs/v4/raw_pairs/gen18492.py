from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.col("customer_id") == 11) & F.month("start_date").isin([3, 4, 10]))
    .withColumn("status_upper", F.upper("status"))
)

summary = (
    base
    .groupBy("plan")
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", 1).otherwise(0)).alias("enterprise_count"),
    )
)

result = (
    summary
    .orderBy("plan", "enterprise_count")
    .limit(3)
)
