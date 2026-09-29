from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.sum("stock_qty").alias("sum_product_stock_qty"))

valid = (
    df_products
    .filter(F.col("launch_date") >= F.to_date(F.lit("2023-05-30")))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.coalesce("price", F.lit(0)).alias("price_filled"),
        F.sort_array("tags").alias("sorted_tags"),
    )
)

joined = (
    valid
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id") == F.col("d.id"), "left")
    .filter(F.col("d.rating") <= 3.7)
    .select("s.*", F.col("d.rating"))
)

with_latest = (
    joined
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.unit_price"),
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

result = (
    latest_match
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.col("sum_product_stock_qty"),
    )
)
