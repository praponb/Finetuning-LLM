from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

filtered = (
    df_orders
    .filter(F.col("status") == "PENDING")
    .select(
        "*",
        F.round((F.col("discount") * 0.9), 2).alias("discount_adjusted"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.shipped_date") <= F.col("s.order_date")), "left_semi")
)

attributed = (
    eligible
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.order_date") >= F.col("pr.starts_on")) & (F.col("e.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

cleaned = (
    attributed
    .filter(F.col("channel").isNotNull() & (F.col("shipping_fee") <= 32))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "amount",
        "discount_adjusted",
        "resolved_channel",
        "promo_id",
        "promo_code",
        "discount_pct",
        (F.col("amount") * F.col("promo_id")).alias("amount_times_promo_id"),
    )
)

with_window = (
    cleaned
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.order_date") >= F.col("sub.start_date")) & (F.col("src.order_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    with_window
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "amount",
        "resolved_channel",
        "promo_id",
        "discount_pct",
        "amount_times_promo_id",
        "subscription_id",
        F.col("max_account_balance"),
    )
    .orderBy(F.col("store_id").asc_nulls_last(), "amount")
)
