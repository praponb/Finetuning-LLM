from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("product_id").orderBy(F.col("discount").desc_nulls_last())
w2 = Window.partitionBy("order_id", "product_id").orderBy(F.col("shipping_fee").asc_nulls_first())
w3 = Window.partitionBy("order_id").orderBy(F.col("discount").asc_nulls_last())

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("*", F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"))
)

enriched = (
    base
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "inner")
    .filter(F.col("p.customer_id").isNull())
    .select(
        "e.id",
        "e.order_id",
        "e.product_id",
        "e.quantity",
        "e.unit_price",
        "e.unit_price_filled",
        F.col("p.order_date"),
        F.col("p.discount"),
        F.col("p.status"),
    )
)

with_dims = (
    enriched
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select(
        "e.*",
        F.col("d.shipping_fee"),
        F.col("d.order_date").alias("order_order_date"),
    )
)

matched_keys = (
    with_dims
    .select("product_id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("unit_price").between(210, 380))
        .select("product_id", "order_id")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .withColumn("discount_percent_rank", F.percent_rank().over(w_grp))
    .withColumn("shipping_fee_rank", F.rank().over(w2))
)

matched_keys_2 = (
    metrics
    .select("id")
    .subtract(
        df_order_items
        .filter(F.col("product_id").isin([13, 33]))
        .select("id")
    )
)

reconciled_order_items = (
    metrics
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled_order_items
    .select("*", F.percent_rank().over(w3).alias("discount_percent_rank_2"))
)

result = (
    with_windows
    .select(
        "id",
        "order_id",
        "product_id",
        "order_date",
        "discount",
        "status",
        "shipping_fee",
        "order_order_date",
        "discount_percent_rank_2",
        F.when(F.col("status").rlike("^(CANCELLED|COMPLETED)$"), "CORE").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy("product_id")
)
