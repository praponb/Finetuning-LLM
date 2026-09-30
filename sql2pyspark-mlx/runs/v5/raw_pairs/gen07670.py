from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.orderBy("quantity")
unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.avg("unit_price").alias("avg_order_item_unit_price"))

enriched = (
    df_inventory
    .filter(F.col("product_id").isin([1, 3, 9, 10]))
    .withColumn("quantity_bucket", F.ntile(4).over(w))
    .withColumn("quantity_percent_rank", F.percent_rank().over(w2))
)

result = (
    enriched
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity_bucket",
        "quantity_percent_rank",
        F.col("avg_order_item_unit_price"),
    )
    .orderBy(F.col("quantity_percent_rank").desc(), F.col("product_id").desc())
)
