from pyspark.sql import functions as F

remaining_keys = (
    df_orders
    .select("id", "customer_id")
    .intersectAll(
        df_orders
        .filter(F.col("customer_id") == 28)
        .select("id", "customer_id")
    )
)

verified = (
    df_orders
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .cube(F.date_trunc("WEEK", "order_date").alias("week_start"))
    .agg(
        F.stddev_samp("discount").alias("discount_stddev"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.count_if(F.col("channel") == "partner").alias("partner_events"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_rows"),
        F.percentile_approx("shipping_fee", [0.25, 0.5, 0.75]).alias("shipping_fee_quartiles"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_rows"),
    )
    .filter(F.col("pending_rows") >= 1)
    .orderBy(F.col("returned_rows").asc_nulls_last())
)
