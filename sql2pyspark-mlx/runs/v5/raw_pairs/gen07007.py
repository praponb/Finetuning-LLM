from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("brand")
w3 = Window.partitionBy("brand", "category")

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select("*", F.concat("brand", F.lit(" - "), "category").alias("brand_category"))
)

with_latest = (
    valid
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.brand",
        "s.stock_qty",
        "s.launch_date",
        "s.tags",
        "s.brand_category",
        F.col("c.unit_price").alias("order_item_unit_price"),
        F.col("c.quantity"),
    )
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.order_id") == 26), "left_anti")
)

kept_keys = (
    not_flagged
    .select("brand", "category")
    .intersectAll(
        df_products
        .filter(F.col("category") == "Electronics")
        .select("brand", "category")
    )
)

kept_rows = (
    not_flagged
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

eligible = (
    kept_rows
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.order_id").isin([16, 33])), "left_semi")
)

enriched = (
    eligible
    .filter(F.array_contains("tags", "premium"))
    .select(
        "*",
        F.sum("quantity").over(w2).alias("brand_sum_quantity"),
        F.avg("stock_qty").over(w3).alias("brand_avg_stock_qty"),
    )
)

eligible_products = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.store_id").isin([5, 6])), "left_semi")
)

result = (
    eligible_products
    .select(
        "category",
        "tags",
        "order_item_unit_price",
        "quantity",
        "brand_sum_quantity",
        "brand_avg_stock_qty",
    )
)
