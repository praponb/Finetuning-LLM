from pyspark.sql import functions as F

kept_keys = (
    df_orders
    .select("status", "store_id")
    .intersect(
        df_orders
        .filter(F.col("store_id").isin([7, 25]))
        .select("status", "store_id")
    )
)

kept_rows = (
    df_orders
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.date_trunc("MONTH", "order_date").alias("activity_month"),
        F.date_trunc("YEAR", "order_date").alias("year_start"),
        F.date_trunc("WEEK", "order_date").alias("week_start"),
    )
    .agg(
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.avg("discount").alias("avg_discount_2"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.avg(F.when(F.col("status") == "RETURNED", F.col("shipping_fee"))).alias("returned_avg_shipping_fee"),
        F.count_if(F.col("status") == "RETURNED").alias("returned_events"),
    )
)
