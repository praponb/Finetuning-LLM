from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter(F.col("discount") >= 0.36)
    .withColumn("is_shipped", F.when(F.col("status") == "SHIPPED", 1).otherwise(0))
)

matched = (
    filtered
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & F.col("e.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

remaining_keys = (
    matched
    .select("store_id", "customer_id")
    .exceptAll(
        df_orders
        .filter(~F.col("channel").isin(["web", "partner"]))
        .select("store_id", "customer_id")
    )
)

reconciled = (
    matched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.quantity") <= 4), "left_anti")
)

result = (
    eligible
    .select(
        "customer_id",
        "order_date",
        "status",
        "discount",
        "shipping_fee",
        "promo_id",
        "promo_code",
        "discount_pct",
        F.when(F.col("status").rlike("^(SHIPPED|PENDING)$"), "CORE").when(F.col("status").rlike("^(COMPLETED|CANCELLED)$"), "PRIORITY").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy(F.col("discount_pct").asc_nulls_first())
)
