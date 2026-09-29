from pyspark.sql import functions as F

metrics_long = (
    df_orders
    .select("id", "status", "discount", "amount")
    .unpivot(["id", "status"], ["discount", "amount"], "measure", "metric_value")
    .filter(F.col("metric_value").isNotNull())
)

result = (
    metrics_long
    .withColumn("metric_value_tier", F.when(F.col("metric_value") >= 67, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["measure", "metric_value_tier"], ["measure"]],
        "measure",
        "metric_value_tier",
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.max(F.when(F.col("measure") == "discount", F.col("metric_value"))).alias("discount_max_metric_value"),
        F.sum(F.when(F.col("measure") == "discount", F.col("metric_value")).otherwise(0)).alias("discount_metric_value"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("metric_value"))).alias("completed_total_metric_value"),
    )
    .orderBy(F.col("metric_value_tier").desc())
)
