from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("store_id")
w3 = Window.orderBy("last_restock_minus_47d", "id").rowsBetween(-3, Window.currentRow)
w_running = Window.partitionBy("product_id").orderBy("cost", "id")
unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.sum("unit_price").alias("sum_order_item_unit_price"))

base = (
    df_inventory
    .filter(F.col("quantity").between(155, 181))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.date_sub("last_restock", 47).alias("last_restock_minus_47d"),
    )
)

with_dims = (
    base
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.cost"))
)

metrics = (
    with_dims
    .select(
        "id",
        "store_id",
        "product_id",
        F.avg("cost").over(w).alias("cost_avg_2"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
        F.avg("cost").over(w3).alias("cost_avg_3"),
        F.row_number().over(w_running).alias("event_seq"),
    )
)

with_dims_inventory = (
    metrics
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .filter(F.length(F.col("d.product_name")) < 12)
    .select("e.*", F.col("d.product_name"), F.col("d.stock_qty"))
)

result = (
    with_dims_inventory
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "cost_avg_2",
        "cost_share",
        "event_seq",
        "stock_qty",
        F.col("sum_order_item_unit_price"),
    )
)
