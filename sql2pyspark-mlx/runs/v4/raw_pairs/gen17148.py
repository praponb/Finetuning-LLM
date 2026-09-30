from pyspark.sql import functions as F

remaining_keys = (
    df_orders
    .select("customer_id")
    .intersectAll(
        df_orders
        .filter(F.col("customer_id").isin([4, 24]))
        .select("customer_id")
    )
)

reconciled = (
    df_orders
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("discount")).otherwise(0)).alias("completed_discount"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_rows"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.stddev_samp("discount").alias("discount_stddev"),
    )
    .orderBy("num_rows")
)
