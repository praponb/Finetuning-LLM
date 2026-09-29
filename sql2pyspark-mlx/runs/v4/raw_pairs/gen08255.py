from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "attr_key")
w2 = Window.partitionBy("attr_key")
w3 = Window.partitionBy("service")
w_ordered = Window.orderBy("status_code")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

metrics = (
    exploded
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w)), 4))
    .withColumn("attr_key_min_status_code", F.min("status_code").over(w2))
    .withColumn("service_avg_status_code", F.avg("status_code").over(w3))
    .withColumn("status_code_rank", F.rank().over(w_ordered))
)

filtered = (
    metrics
    .filter(F.col("attr_key").isNotNull())
    .withColumn("status_code_rank_str", F.col("status_code_rank").cast("double"))
)

result = (
    filtered
    .groupBy("service")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("attr_key_min_status_code", 0.95).alias("attr_key_min_status_code_p95"),
        F.sum(F.when(F.col("service") == "billing", F.col("service_avg_status_code")).otherwise(0)).alias("billing_service_avg_status_code"),
        F.count(F.when(F.col("service") == "checkout", 1)).alias("checkout_rows"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
    )
    .orderBy(F.col("checkout_rows").desc())
)
