from pyspark.sql import functions as F

metrics_long = (
    df_orders
    .select(
        "id",
        "channel",
        F.col("amount").alias("AMOUNT_VAL"),
        F.col("shipping_fee").alias("SHIPPING_FEE"),
    )
    .unpivot(
        ["id", "channel"],
        ["AMOUNT_VAL", "SHIPPING_FEE"],
        "metric_name",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)

result = (
    metrics_long
    .cube("metric_name")
    .agg(
        F.max("metric_value").alias("max_metric_value"),
        F.sum(F.when(F.col("metric_name") == "AMOUNT_VAL", F.col("metric_value")).otherwise(0)).alias("amount_val_metric_value"),
        F.count("*").alias("row_count"),
        F.round(F.sum("metric_value") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("metric_value_per_web"),
        F.percentile_approx("metric_value", [0.25, 0.5, 0.75]).alias("metric_value_quartiles"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("row_count") >= 1)
    .orderBy("amount_val_metric_value")
)
