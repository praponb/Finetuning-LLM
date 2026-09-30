from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "store_id").orderBy("order_date", "id")

ranked = (
    df_orders
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "order_date", "status", "channel", "amount", "shipping_fee")
)

result = (
    ranked
    .select("id", "order_date", "status", "amount", "shipping_fee")
    .orderBy(F.col("shipping_fee").desc_nulls_last())
)
