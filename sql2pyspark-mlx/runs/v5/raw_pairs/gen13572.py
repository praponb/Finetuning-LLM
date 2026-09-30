from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("category").orderBy("inventory_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("inventory_last_restock").desc(), "id")
w4 = Window.partitionBy("category").orderBy(F.col("seq_no").desc())

valid = (
    df_products
    .filter(F.col("cost") >= 640)
    .withColumn("greatest_stock_qty_cost", F.greatest("stock_qty", "cost"))
)

with_latest = (
    valid
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.launch_date",
        "s.tags",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

scored = (
    with_latest
    .filter(F.col("inventory_quantity") != 116)
    .withColumn("running_inventory_quantity", F.sum("inventory_quantity").over(w_running))
    .withColumn("seq_no", F.row_number().over(w3))
)

latest = (
    scored
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

eligible = (
    latest
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.order_id").isin([27, 39, 40])), "left_anti")
)

with_last = (
    eligible
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity"),
        F.col("c.last_restock").alias("inventory_last_restock_2"),
    )
)

with_last_products = (
    with_last
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.launch_date",
        "s.tags",
        "s.inventory_last_restock",
        "s.running_inventory_quantity",
        "s.seq_no",
        "s.quantity",
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

result = (
    with_last_products
    .groupBy(
        F.date_trunc("week", "inventory_last_restock").alias("week_start"),
        F.when(F.col("order_item_quantity") >= 8, "HIGH").otherwise("LOW").alias("order_item_quantity_tier"),
    )
    .agg(
        F.size(F.collect_set("category")).alias("num_categories"),
        F.size(F.collect_set("category")).alias("num_categories_2"),
    )
    .filter(F.col("num_categories") > 1)
    .orderBy("order_item_quantity_tier")
)
