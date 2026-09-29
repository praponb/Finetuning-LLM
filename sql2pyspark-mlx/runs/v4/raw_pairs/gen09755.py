from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipping_fee", "id")
w2 = Window.partitionBy("store_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_orders
    .withColumn("shipping_fee_bucket", F.ntile(3).over(w))
    .withColumn("cumulative_discount", F.sum("discount").over(w2))
)
