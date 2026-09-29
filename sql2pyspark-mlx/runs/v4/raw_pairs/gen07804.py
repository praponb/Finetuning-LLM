from pyspark.sql import functions as F

result = (
    df_subscriptions
    .withColumn("week_start", F.date_trunc("week", "start_date"))
    .rollup("week_start")
    .agg(
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_count"),
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_rows"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.grouping("week_start").alias("grouping_level"),
    )
    .orderBy("week_start")
)
