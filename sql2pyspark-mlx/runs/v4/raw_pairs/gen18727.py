from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w3 = Window.orderBy(F.col("unit_price").desc(), "id")

result = (
    df_order_items
    .withColumn("unit_price_bucket", F.ntile(5).over(w_running))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)
