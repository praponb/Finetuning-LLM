from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy("promo_code")
w3 = Window.partitionBy("store_id", "promo_code").orderBy("shipping_fee", "id", "promo_id")
w4 = Window.partitionBy("store_id").orderBy("corr_discount_shipping_fee_rounded", "id", "promo_id").rowsBetween(-5, -1)
w5 = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc(), "id", "promo_id")
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.avg("quantity").alias("avg_inventory_quantity"))

matched = (
    df_orders
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & F.col("s.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

enriched = (
    matched
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        "promo_id",
        "promo_code",
        F.corr("discount", "shipping_fee").over(w).alias("corr_discount_shipping_fee"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share"),
    )
)

base = (
    enriched
    .filter(F.col("promo_id") == 3)
    .withColumn("corr_discount_shipping_fee_rounded", F.round("corr_discount_shipping_fee", 0))
)

not_flagged = (
    base
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.quantity").between(4, 8)), "left_anti")
)

metrics = (
    not_flagged
    .filter(F.col("status") == "CANCELLED")
    .select(
        "*",
        F.lead("corr_discount_shipping_fee", 1).over(w3).alias("next_corr_discount_shipping_fee"),
        F.avg("promo_id").over(w4).alias("promo_id_avg_5_prior"),
        F.ntile(3).over(w5).alias("shipping_fee_bucket"),
    )
)

result = (
    metrics
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "promo_id",
        "corr_discount_shipping_fee",
        "discount_pct_share",
        "next_corr_discount_shipping_fee",
        "promo_id_avg_5_prior",
        F.col("avg_inventory_quantity"),
    )
    .orderBy(F.col("promo_id").asc_nulls_last(), F.col("id").desc())
    .limit(5)
)
