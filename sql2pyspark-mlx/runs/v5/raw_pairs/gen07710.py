from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_last(), "id")
w2 = Window.orderBy(F.col("shipping_fee").asc_nulls_first())
w3 = Window.orderBy(F.col("discount").desc_nulls_last())
w_ordered = Window.partitionBy("order_id")

latest = (
    df_order_items
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "order_id", "product_id", "quantity")
)

remaining_keys = (
    latest
    .select("id", "order_id")
    .intersect(
        df_order_items
        .filter(F.col("product_id") == 16)
        .select("id", "order_id")
    )
)

verified = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .filter(F.col("p.amount").isNotNull())
    .select("e.id", "e.order_id", F.col("p.shipping_fee"), F.col("p.discount"))
)

scored = (
    enriched
    .select(
        "id",
        "discount",
        F.rank().over(w2).alias("shipping_fee_rank"),
        F.cume_dist().over(w3).alias("discount_cume_dist"),
        F.stddev_samp("shipping_fee").over(w_ordered).alias("order_id_stddev_samp_shipping_fee"),
    )
)

result = (
    scored
    .select("id", "discount", "shipping_fee_rank", "order_id_stddev_samp_shipping_fee")
    .orderBy(F.col("shipping_fee_rank").asc_nulls_last())
)
