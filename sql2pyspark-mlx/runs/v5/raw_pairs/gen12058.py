from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc(), "id")
shipping_fee_by_store_id = df_orders.groupBy("store_id").agg(F.min("shipping_fee").alias("min_order_shipping_fee"))

ranked = (
    df_inventory
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .join(shipping_fee_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "row_num",
        F.col("min_order_shipping_fee"),
    )
)
