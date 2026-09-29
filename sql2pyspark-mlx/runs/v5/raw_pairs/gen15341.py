from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id").orderBy("amount", "id")
w_hist_2 = Window.partitionBy("status", "store_id")
w3 = Window.partitionBy("status").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_orders
    .withColumn("amount_bucket", F.ntile(4).over(w_hist))
    .withColumn("status_stddev_samp_shipping_fee", F.stddev_samp("shipping_fee").over(w_hist_2))
    .withColumn("running_shipping_fee", F.sum("shipping_fee").over(w3))
)
