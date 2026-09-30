from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").asc_nulls_last(), "id")
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.avg("quantity").alias("avg_order_item_quantity"))

latest = (
    df_order_items
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.col("avg_order_item_quantity"),
    )
    .orderBy(F.col("order_id").asc_nulls_first())
)
