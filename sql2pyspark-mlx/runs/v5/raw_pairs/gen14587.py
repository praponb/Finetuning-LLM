from pyspark.sql import functions as F

kept_keys = (
    df_payments
    .select("method", "order_id")
    .intersectAll(
        df_payments
        .filter(F.col("status") == "FAILED")
        .select("method", "order_id")
    )
)

reconciled = (
    df_payments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .cube(F.when(F.col("amount") >= 1340, "HIGH").otherwise("LOW").alias("amount_tier"))
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.avg("amount").alias("avg_amount"),
        F.count_if(F.col("status") == "FAILED").alias("failed_events"),
        F.min("paid_at").alias("first_paid_at"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") < 1000)
    .orderBy("avg_amount")
)
