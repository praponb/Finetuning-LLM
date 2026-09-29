from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("quantity").desc())
w2 = Window.partitionBy("order_id")
w_running_2 = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy(F.col("quantity").asc_nulls_first())
w5 = Window.partitionBy("product_id")

result = (
    df_order_items
    .withColumn("quantity_percent_rank", F.percent_rank().over(w_running))
    .withColumn("corr_unit_price_quantity", F.corr("unit_price", "quantity").over(w2))
    .withColumn("running_quantity", F.sum("quantity").over(w_running_2))
    .withColumn("quantity_rank", F.rank().over(w4))
    .withColumn("product_id_sum_unit_price", F.sum("unit_price").over(w5))
)
