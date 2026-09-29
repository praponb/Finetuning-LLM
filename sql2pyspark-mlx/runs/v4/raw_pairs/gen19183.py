from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("order_id")
w2 = Window.partitionBy("status")
w_grp = Window.orderBy(F.col("discount").desc())
w4 = Window.partitionBy("order_id", "carrier")

joined = (
    df_shipments
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.discount"), F.col("b.amount"), F.col("b.shipping_fee"))
)

enriched = (
    joined
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.discount").alias("order_discount"), F.col("b.order_date"))
)

remaining_keys = (
    enriched
    .select("status", "carrier")
    .subtract(
        df_shipments
        .filter(F.month("shipped_date").isin([4, 9, 10]))
        .select("status", "carrier")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "*",
        F.min("shipping_fee").over(w_hist).alias("order_id_min_shipping_fee"),
        F.min("discount").over(w2).alias("status_min_discount"),
        F.percent_rank().over(w_grp).alias("discount_percent_rank"),
        F.stddev_samp("shipping_fee").over(w4).alias("order_id_stddev_samp_shipping_fee"),
    )
)

result = (
    scored
    .select(
        "status",
        F.col("shipping_fee").alias("SHIPPING_FEE_VAL"),
        F.col("discount_percent_rank").alias("DISCOUNT_PERCENT_RANK_VAL"),
        "order_discount",
    )
    .unpivot(
        ["status"],
        ["SHIPPING_FEE_VAL", "DISCOUNT_PERCENT_RANK_VAL", "order_discount"],
        "metric_name",
        "val",
    )
    .filter(F.col("val").isNotNull())
)
