from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("stock_qty").desc_nulls_first())

base = (
    df_order_items
    .filter(F.col("unit_price") < 250)
    .select(
        "id",
        "product_id",
        "quantity",
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
    )
)

enriched = (
    base
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .filter(F.col("d.price") > 620)
    .select(
        "s.*",
        F.col("d.stock_qty"),
        F.col("d.launch_date"),
        F.col("d.product_name"),
    )
)

with_dims = (
    enriched
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "s.id",
        "s.product_id",
        "s.unit_price_filled",
        "s.launch_date",
        F.col("p.category"),
        F.col("p.product_name").alias("product_product_name"),
    )
)

with_dims_order_items = (
    with_dims
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .select("e.id", "e.product_id", "e.product_product_name", F.col("d.stock_qty"))
)

enriched_order_items = (
    with_dims_order_items
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.price"), F.col("b.cost"))
)

enriched_order_items_2 = (
    enriched_order_items
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id").eqNullSafe(F.col("b.id")), "inner")
    .select(
        "src.id",
        "src.product_id",
        "src.product_product_name",
        "src.stock_qty",
        F.col("b.category"),
    )
)

remaining_keys = (
    enriched_order_items_2
    .select("id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") >= 3)
        .select("id", "product_id")
    )
)

reconciled = (
    enriched_order_items_2
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "product_id",
        "product_product_name",
        F.percent_rank().over(w).alias("stock_qty_percent_rank"),
    )
)
