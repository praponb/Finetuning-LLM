from pyspark.sql import functions as F

remaining_keys = (
    df_orders
    .select("customer_id", "channel")
    .exceptAll(
        df_orders
        .filter(F.col("customer_id").isin([1, 6, 23]))
        .select("customer_id", "channel")
    )
)

verified = (
    df_orders
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("quarter_start", F.date_trunc("QUARTER", "order_date"))
    .rollup("quarter_start")
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.count_if(F.col("status") == "COMPLETED").alias("completed_events"),
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
        F.corr("amount", "discount").alias("corr_amount_discount"),
        F.grouping_id().alias("gid"),
    )
    .orderBy("corr_amount_discount")
)
