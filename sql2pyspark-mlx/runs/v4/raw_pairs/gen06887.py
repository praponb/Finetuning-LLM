from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("channel")
w_recent = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy(F.col("discount_plus_amount").asc_nulls_first())
w4 = Window.partitionBy("customer_id").orderBy("discount", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("channel").orderBy("amount", "id", "promo_id")
w7 = Window.partitionBy("customer_id")
w_hist = Window.orderBy(F.col("payment_amount").desc_nulls_first())
w9 = Window.partitionBy("store_id")
w10 = Window.partitionBy("store_id").orderBy("amount", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_orders
    .filter(F.col("channel").isNotNull() & F.col("order_date").between("2023-04-01", "2025-01-30"))
    .select(
        "*",
        F.date_format("order_date", "yyyy-MM").alias("order_date_label"),
        (F.col("discount") + F.col("amount")).alias("discount_plus_amount"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.min("amount").over(w_part).alias("channel_min_amount"),
        F.corr("discount", "amount").over(w_part).alias("corr_discount_amount"),
        F.sum("shipping_fee").over(w_recent).alias("cumulative_shipping_fee"),
        F.dense_rank().over(w3).alias("discount_plus_amount_dense_rank"),
        F.ntile(10).over(w4).alias("discount_bucket"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("payment_amount"))
)

with_window = (
    with_last
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("e.order_date") >= F.col("pr.starts_on")) & (F.col("e.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

with_windows = (
    with_window
    .withColumn("prev_shipping_fee", F.lag("shipping_fee", 2).over(w6))
    .withColumn("customer_id_min_discount_bucket", F.min("discount_bucket").over(w7))
)

with_windows_orders = (
    with_windows
    .select(
        "*",
        F.rank().over(w_hist).alias("payment_amount_rank"),
        F.corr("amount", "discount").over(w9).alias("corr_amount_discount"),
        F.sum("promo_id").over(w10).alias("cumulative_promo_id"),
    )
)

result = (
    with_windows_orders
    .rollup("cumulative_promo_id", "status")
    .agg(
        F.count_if(F.col("channel") == "partner").alias("partner_events"),
        F.avg(F.when(F.col("channel") == "web", F.col("cumulative_promo_id"))).alias("web_avg_cumulative_promo_id"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.sum(F.when(F.col("channel") == "web", F.col("payment_amount_rank")).otherwise(0)).alias("web_payment_amount_rank"),
    )
    .orderBy(F.col("distinct_store_ids").desc_nulls_first())
)
