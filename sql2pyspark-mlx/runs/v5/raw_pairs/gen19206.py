from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc())
w3 = Window.partitionBy("channel", "customer_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .withColumn("cumulative_shipping_fee", F.sum("shipping_fee").over(w))
    .withColumn("shipping_fee_percent_rank", F.percent_rank().over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)
