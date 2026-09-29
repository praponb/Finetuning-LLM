from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("amount", "id")
w2 = Window.orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_orders
    .withColumn("next_shipping_fee", F.lead("shipping_fee", 1).over(w))
    .withColumn("cumulative_shipping_fee", F.sum("shipping_fee").over(w2))
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w3))
)
