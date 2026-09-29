from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

matched_keys = (
    valid
    .select("customer_id", "id")
    .subtract(
        df_orders
        .filter(F.col("customer_id") == 22)
        .select("customer_id", "id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.carrier").isin(["UPS", "Kerry"])), "left_anti")
)

attributed = (
    eligible
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & F.col("s.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    attributed
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "amount",
        "amount_filled",
        "channel_filled",
        "promo_id",
        "promo_code",
        F.when(F.col("status").rlike("^(CANCELLED|COMPLETED)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("status_zone"),
    )
    .orderBy(F.col("channel").asc_nulls_first())
)
