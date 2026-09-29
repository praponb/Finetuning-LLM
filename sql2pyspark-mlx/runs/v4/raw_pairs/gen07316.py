from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_last(), "id")
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

latest = (
    df_order_items
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .join(quantity_by_order_id, on="order_id", how="left")
    .select("id", "order_id", "quantity", F.col("sum_order_item_quantity"))
    .orderBy(F.col("id").desc_nulls_last(), "sum_order_item_quantity")
)
