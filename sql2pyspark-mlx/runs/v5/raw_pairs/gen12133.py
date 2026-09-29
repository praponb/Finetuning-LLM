from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category").orderBy("stock_qty", "id")
w3 = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_products
    .filter(F.col("launch_date") <= "2024-08-01")
    .select(
        "*",
        F.concat("category", F.lit(" "), "product_name").alias("category_product_name"),
        F.quarter("launch_date").alias("launch_date_quarter"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_filled"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.brand",
        "s.price",
        "s.stock_qty",
        "s.tags",
        "s.supplier_id",
        "s.category_product_name",
        F.col("c.unit_price"),
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

eligible = (
    with_last
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.last_restock").between("2024-09-01", "2025-05-01")), "left_anti")
)

metrics = (
    eligible
    .select(
        "id",
        "stock_qty",
        "tags",
        "supplier_id",
        "category_product_name",
        "unit_price",
        "order_item_quantity",
        F.lead("stock_qty", 1).over(w2).alias("next_stock_qty"),
        F.last("unit_price", ignorenulls=True).over(w3).alias("last_known_unit_price"),
    )
)

result = (
    metrics
    .groupBy("supplier_id")
    .agg(
        F.countDistinct("supplier_id").alias("distinct_supplier_ids"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
    )
    .orderBy(F.col("stock_qty_bit_and").desc_nulls_first())
)
