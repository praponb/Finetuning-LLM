from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())
unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.sum("unit_price").alias("sum_order_item_unit_price"))

metrics = (
    df_inventory
    .filter(F.col("quantity").between(61, 156))
    .select(
        "id",
        "store_id",
        "product_id",
        F.lag("quantity", 1).over(w).alias("prev_quantity"),
        F.ntile(10).over(w2).alias("quantity_bucket"),
        F.cume_dist().over(w3).alias("quantity_cume_dist"),
    )
)

result = (
    metrics
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "prev_quantity",
        "quantity_bucket",
        "quantity_cume_dist",
        F.col("sum_order_item_unit_price"),
    )
    .orderBy(F.col("id").desc_nulls_last())
    .limit(10)
)
