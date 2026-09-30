from pyspark.sql import functions as F

result = (
    df_payments
    .withColumn("amount_tier", F.when(F.col("amount") >= 1730, "HIGH").otherwise("LOW"))
    .rollup("method", "amount_tier")
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_rows"),
        F.grouping("method").alias("grouping_id"),
    )
    .orderBy("num_status")
)
