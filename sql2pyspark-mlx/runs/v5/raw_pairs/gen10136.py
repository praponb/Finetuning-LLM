from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("discount", "id", "promo_id")
w2 = Window.partitionBy(F.col("s.id"), F.col("s.promo_id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())
w3 = Window.orderBy("discount", "id", "promo_id").rowsBetween(-2, -1)
w4 = Window.orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_window = (
    df_orders
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & F.col("a.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

deduped = (
    with_window
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.unit_price").between(150, 210)), "left_anti")
)

with_latest = (
    not_flagged
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.method"), F.col("c.status").alias("payment_status"))
)

not_flagged_orders = (
    with_latest
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.product_id") == 36), "left_anti")
)

with_windows = (
    not_flagged_orders
    .select(
        "id",
        "store_id",
        "order_date",
        "discount",
        "shipping_fee",
        "promo_id",
        "dedup_rank",
        "method",
        "payment_status",
        F.avg("discount_pct").over(w3).alias("discount_pct_avg_2_prior"),
        F.first("discount", ignorenulls=True).over(w4).alias("first_known_discount"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "store_id",
        "discount",
        "shipping_fee",
        "promo_id",
        "discount_pct_avg_2_prior",
        "first_known_discount",
    )
    .orderBy(F.col("discount").desc_nulls_first())
)
