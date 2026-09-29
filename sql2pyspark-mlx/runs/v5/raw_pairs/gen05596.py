from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").asc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("cost").alias("max_product_cost"))

base = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-05-15").cast("date")) & F.col("supplier_id").isNotNull())
    .withColumn("category_prefix", F.substring("category", 1, 2))
)

deduped = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "category", "stock_qty", "supplier_id")
)

with_last = (
    deduped
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_id",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock"),
    )
)

with_last_products = (
    with_last
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_id",
        "s.last_restock",
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

result = (
    with_last_products
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select("id", "supplier_id", F.col("max_product_cost"))
)
