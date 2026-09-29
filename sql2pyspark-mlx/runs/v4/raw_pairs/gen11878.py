from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("order_date").desc(), "id")
w2 = Window.partitionBy("status").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_orders
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("running_shipping_fee", F.sum("shipping_fee").over(w2))
)
