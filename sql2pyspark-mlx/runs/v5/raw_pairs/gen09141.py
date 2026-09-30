from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")

result = (
    df_order_items
    .withColumn("max_quantity_by_order", F.max("quantity").over(w))
    .filter(F.col("quantity") == F.col("max_quantity_by_order"))
    .select("id", "order_id", "quantity", "product_id")
)
