from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("product_id", "status")
w2 = Window.orderBy("amount")
w3 = Window.partitionBy("channel", "product_id")
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.max("quantity").alias("max_inventory_quantity"))

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
        F.round("unit_price", 2).alias("unit_price_rounded"),
    )
)

kept_keys = (
    base
    .select("product_id", "order_id")
    .intersectAll(
        df_order_items
        .filter(F.col("order_id").isin([11, 33]))
        .select("product_id", "order_id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.customer_id") == 27)
    .select("s.*", F.col("o.amount"), F.col("o.order_date"), F.col("o.channel"))
)

joined = (
    enriched
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.product_id",
        "a.unit_price",
        "a.line_total",
        "a.amount",
        "a.order_date",
        "a.channel",
        F.col("o.status"),
        F.col("o.discount"),
    )
)

enriched_order_items = joined.select("*", F.avg("discount").over(w_part).alias("product_id_avg_discount"))

metrics = (
    enriched_order_items
    .select(
        "order_id",
        "product_id",
        "unit_price",
        "line_total",
        "order_date",
        "product_id_avg_discount",
        F.cume_dist().over(w2).alias("amount_cume_dist"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w3)), 4).alias("unit_price_share"),
    )
)

enriched_order_items_2 = (
    metrics
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select(
        "a.product_id",
        "a.unit_price",
        "a.amount_cume_dist",
        F.col("d.product_name"),
        F.col("d.cost"),
    )
)

result = (
    enriched_order_items_2
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "unit_price",
        "amount_cume_dist",
        "product_name",
        "cost",
        F.col("max_inventory_quantity"),
    )
    .orderBy(F.col("amount_cume_dist").asc_nulls_first(), "product_id")
)
