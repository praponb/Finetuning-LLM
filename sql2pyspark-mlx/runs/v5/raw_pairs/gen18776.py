from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("shipping_fee").desc_nulls_last())
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("store_id").orderBy("shipping_fee", "id").rowsBetween(-6, -1)
w_running = Window.partitionBy("channel").orderBy(F.col("shipping_fee_dense_rank").desc(), "id", "promo_id")
w5 = Window.orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

eligible = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") < F.col("s.order_date")), "left_semi")
)

with_windows = (
    eligible
    .select(
        "*",
        F.dense_rank().over(w).alias("shipping_fee_dense_rank"),
        F.corr("amount", "discount").over(w2).alias("corr_amount_discount"),
        F.avg("amount").over(w3).alias("amount_avg_6_prior"),
    )
)

attributed = (
    with_windows
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & F.col("s.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

metrics = (
    attributed
    .select(
        "*",
        F.ntile(10).over(w_running).alias("shipping_fee_dense_rank_bucket"),
        F.last("discount", ignorenulls=True).over(w5).alias("last_known_discount"),
    )
)

result = (
    metrics
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "shipping_fee_dense_rank",
        "corr_amount_discount",
        "amount_avg_6_prior",
        "promo_id",
        F.when(F.col("status").rlike("^(SHIPPED|COMPLETED)$"), "EXTENDED").otherwise("PRIORITY").alias("status_zone"),
    )
)
