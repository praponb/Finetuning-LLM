from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "store_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel", "customer_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id", "status").orderBy("shipping_fee", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("status").orderBy("discount", "id")

result = (
    df_orders
    .withColumn("first_known_shipping_fee", F.first("shipping_fee", ignorenulls=True).over(w))
    .withColumn("first_known_discount", F.first("discount", ignorenulls=True).over(w2))
    .withColumn("first_known_discount_2", F.first("discount", ignorenulls=True).over(w3))
    .withColumn("amount_avg_3_prior", F.avg("amount").over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
)
