from pyspark.sql import functions as F

kept_keys = (
    df_orders
    .select("channel")
    .intersect(
        df_orders
        .filter(F.col("shipping_fee") < 35)
        .select("channel")
    )
)

kept_rows = (
    df_orders
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("WEEK", "order_date").alias("week_start"))
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("amount"))).alias("completed_total_amount"),
        F.avg(F.when(F.col("channel") == "store", F.col("shipping_fee"))).alias("store_avg_shipping_fee"),
        F.percentile_approx("discount", 0.95).alias("discount_p95"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.corr("discount", "shipping_fee").alias("corr_discount_shipping_fee"),
    )
    .filter(F.col("discount_p95") >= 0)
)
