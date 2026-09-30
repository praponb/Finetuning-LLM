from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

with_dims = (
    df_products
    .alias("src")
    .join(df_suppliers.alias("b"), F.col("src.supplier_id").eqNullSafe(F.col("b.id")), "inner")
    .select(
        "src.id",
        "src.stock_qty",
        "src.tags",
        "src.supplier_id",
        F.col("b.rating"),
        F.col("b.city"),
        F.col("b.supplier_name"),
    )
)

cleaned = (
    with_dims
    .filter(F.col("supplier_id").isin([16, 30]))
    .select(
        "id",
        "stock_qty",
        "tags",
        "supplier_id",
        "rating",
        "city",
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
        F.coalesce("city", F.lit("DIRECT")).alias("resolved_city"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.tags",
        "s.rating",
        "s.city_filled",
        "s.resolved_city",
        F.col("c.unit_price").alias("order_item_unit_price"),
        F.col("c.quantity"),
    )
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.store_id").isin([4, 5])), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "tags",
        "rating",
        "city_filled",
        "resolved_city",
        "order_item_unit_price",
    )
    .orderBy(F.col("rating").desc())
)
