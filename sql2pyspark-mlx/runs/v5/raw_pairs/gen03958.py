from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").asc_nulls_last(), "id")
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.max("quantity").alias("max_order_item_quantity"))

ranked = (
    df_payments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .join(quantity_by_order_id, on="order_id", how="left")
    .select("id", "order_id", "paid_at", "status", F.col("max_order_item_quantity"))
    .orderBy("max_order_item_quantity", "id")
)
