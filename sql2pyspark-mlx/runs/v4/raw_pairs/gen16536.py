from pyspark.sql import functions as F

unpivoted = (
    df_orders
    .select("id", "customer_id", "discount", "shipping_fee")
    .unpivot(["id", "customer_id"], ["discount", "shipping_fee"], "metric", "val")
    .filter(F.col("val").isNotNull())
)

result = (
    unpivoted
    .groupBy("metric")
    .agg(
        F.size(F.collect_set("metric")).alias("num_metrics"),
        F.sum(F.when(F.col("metric") == "shipping_fee", F.col("val")).otherwise(0)).alias("shipping_fee_val"),
        F.percentile_approx("val", 0.9).alias("val_p90"),
        F.count(F.when(F.col("metric") == "discount", 1)).alias("discount_count"),
        F.sum(F.when(F.col("metric") == "discount", F.col("val")).otherwise(0)).alias("discount_val"),
    )
    .orderBy(F.col("val_p90").desc())
)
