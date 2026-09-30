from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("order_date") >= F.to_date(F.lit("2023-07-25")))
    .select("*", F.initcap(F.trim("status")).alias("status_title"))
)

attributed = (
    valid
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("a.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

with_window = (
    attributed
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.order_date") >= F.col("pr.starts_on")) & (F.col("src.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

not_flagged = (
    with_window
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.order_id") == 22), "left_anti")
)

cleaned = (
    not_flagged
    .filter(F.col("store_id").isNotNull() & (~F.col("promotion_promo_code").eqNullSafe(F.lit("WELCOME5"))))
    .withColumn("discount_pct_int", F.col("discount_pct").cast("int"))
    .withColumn("discount_per_promo_id_2", F.round((F.col("discount") / F.col("promo_id_2")), 2))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("DIRECT")))
)

result = (
    cleaned
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "amount",
        "promo_id",
        "promo_code",
        "discount_pct",
        F.when(F.col("status").rlike("^(COMPLETED|PENDING)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("status_zone"),
    )
    .orderBy(F.col("promo_id").asc_nulls_last(), F.col("store_id").desc())
)
