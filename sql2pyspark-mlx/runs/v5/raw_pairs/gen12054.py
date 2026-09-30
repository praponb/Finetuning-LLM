from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

cleaned = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & F.array_contains("tags", "premium"))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("Unknown")))
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("order_item_quantity"),
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

kept = (
    with_latest
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.sale_date") >= F.col("s.launch_date")), "left_semi")
)

with_last = (
    kept
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.brand",
        "s.price",
        "s.stock_qty",
        "s.launch_date",
        "s.tags",
        "s.brand_filled",
        "s.order_item_quantity",
        "s.order_item_unit_price",
        F.col("c.unit_price").alias("order_item_unit_price_2"),
        F.col("c.quantity"),
    )
)

result = (
    with_last
    .select(
        "id",
        "category",
        "brand",
        "price",
        "launch_date",
        "order_item_quantity",
        "order_item_unit_price",
        F.when(F.col("category").rlike("^(Sports|Home)$"), "GROUP_A").otherwise("CORE").alias("category_zone"),
    )
    .orderBy(F.col("order_item_unit_price").desc())
)
