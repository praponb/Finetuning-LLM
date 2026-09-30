from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id")
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").desc_nulls_last())
w4 = Window.partitionBy("product_id")
w5 = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_first())

ranked = (
    df_order_items
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    ranked
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w2)), 4).alias("unit_price_share"),
    )
)

kept_keys = (
    with_windows
    .select("product_id", "id")
    .subtract(
        df_order_items
        .filter(F.col("order_id").isin([3, 5, 23]))
        .select("product_id", "id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "*",
        F.rank().over(w3).alias("quantity_rank"),
        F.max("unit_price_share").over(w4).alias("product_id_max_unit_price_share"),
        F.cume_dist().over(w5).alias("quantity_cume_dist"),
    )
)

remaining_keys = (
    enriched
    .select("order_id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("unit_price") <= 390)
        .select("order_id", "product_id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

enriched_order_items = (
    kept_rows
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.launch_date"))
)

joined = (
    enriched_order_items
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select("a.*", F.col("o.order_date"), F.col("o.status"), F.col("o.shipping_fee"))
)

by_type = (
    joined
    .groupBy("product_id")
    .pivot("status", ["CANCELLED", "SHIPPED"])
    .agg(
        F.sum("unit_price_share").alias("amount"),
        F.count(F.lit(1)).alias("item_count"),
    )
)

result = (
    by_type
    .select("product_id", "CANCELLED_amount", "SHIPPED_amount", "SHIPPED_item_count")
)
