from pyspark.sql import functions as F

eligible = (
    df_orders
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.paid_at") <= F.col("s.order_date")), "left_anti")
)

with_window = (
    eligible
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.order_date") >= F.col("pr.starts_on")) & (F.col("a.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

remaining_keys = (
    with_window
    .select("customer_id", "channel")
    .exceptAll(
        df_orders
        .filter(F.col("order_date") >= "2024-06-16")
        .select("customer_id", "channel")
    )
)

reconciled = (
    with_window
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .cube("status")
    .agg(
        F.countDistinct("channel").alias("distinct_channels"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_rows"),
    )
)
