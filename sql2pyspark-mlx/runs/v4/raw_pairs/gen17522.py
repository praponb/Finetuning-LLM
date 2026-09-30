from pyspark.sql import functions as F

with_window = (
    df_orders
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.order_date") >= F.col("pr.starts_on")) & (F.col("a.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

with_dims = (
    with_window
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "inner")
    .select("s.*", F.col("p2.store_name"))
)

enriched = (
    with_dims
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.segment"), F.col("b.email"))
)

result = (
    enriched
    .select(
        "id",
        "store_id",
        "channel",
        "amount",
        "shipping_fee",
        "promo_id",
        "promo_code",
        "discount_pct",
        "store_name",
        F.when(F.col("store_name").rlike("^(Siam Flagship|Riverside)$"), "CORE").when(F.col("store_name").rlike("^(Downtown Express|Airport Kiosk)$"), "GROUP_A").otherwise("GROUP_B").alias("store_name_zone"),
    )
    .orderBy(F.col("promo_code").desc_nulls_first())
)
