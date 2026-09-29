from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.orderBy(F.col("quantity").desc_nulls_first())
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.min("quantity").alias("min_order_item_quantity"))

metrics = (
    df_order_items
    .filter(F.col("quantity") == 6)
    .select(
        "*",
        F.round((F.col("quantity") / F.sum("quantity").over(w)), 4).alias("quantity_share"),
        F.cume_dist().over(w2).alias("quantity_cume_dist"),
    )
)

result = (
    metrics
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        "quantity_share",
        "quantity_cume_dist",
        F.col("min_order_item_quantity"),
    )
)
