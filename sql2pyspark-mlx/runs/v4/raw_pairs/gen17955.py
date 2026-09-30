from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "enterprise", 1).otherwise(0))) / F.count("*")), 1).alias("pct_enterprise"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy(F.col("customer_id").desc())
)
