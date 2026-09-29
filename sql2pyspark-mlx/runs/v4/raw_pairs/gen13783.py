from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("shipping_fee").desc(), "id", "promo_id")
w2 = Window.partitionBy("customer_id").orderBy("shipping_fee", "id", "promo_id")
w3 = Window.orderBy("shipping_fee", "id", "promo_id").rowsBetween(-2, Window.currentRow)
w4 = Window.orderBy("discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id", "channel")
w6 = Window.partitionBy("channel").orderBy("promo_id", "id", "promo_id").rowsBetween(-5, -1)
w7 = Window.partitionBy("store_id")
w8 = Window.orderBy(F.col("discount_pct_share").asc_nulls_first())
w9 = Window.partitionBy("status")
w10 = Window.partitionBy("customer_id")

matched = (
    df_orders
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.order_date") >= F.col("pr.starts_on")) & (F.col("s.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

kept = (
    matched
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.quantity") > 8), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.paid_at") < F.col("s.order_date")), "left_semi")
)

enriched = (
    not_flagged
    .select(
        "*",
        F.ntile(10).over(w).alias("shipping_fee_bucket"),
        F.ntile(10).over(w2).alias("shipping_fee_bucket_2"),
        F.avg("shipping_fee").over(w3).alias("shipping_fee_avg_2"),
    )
)

enriched_orders = (
    enriched
    .withColumn("first_known_shipping_fee_bucket_2", F.first("shipping_fee_bucket_2", ignorenulls=True).over(w4))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w5)), 4))
    .withColumn("amount_avg_5_prior", F.avg("amount").over(w6))
)

matched_keys = (
    enriched_orders
    .select("status", "store_id")
    .subtract(
        df_orders
        .filter(F.col("discount") > 0.26)
        .select("status", "store_id")
    )
)

kept_rows = (
    enriched_orders
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "*",
        F.round((F.col("shipping_fee_bucket") / F.sum("shipping_fee_bucket").over(w7)), 4).alias("shipping_fee_bucket_share"),
        F.dense_rank().over(w8).alias("discount_pct_share_dense_rank"),
        F.sum("shipping_fee_bucket_2").over(w9).alias("status_sum_shipping_fee_bucket_2"),
        F.round((F.col("promo_id") / F.sum("promo_id").over(w10)), 4).alias("promo_id_share"),
    )
)
