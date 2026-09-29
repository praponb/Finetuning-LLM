from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc(), "id")

ranked = (
    df_orders
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "store_id", "status", "channel", "discount", "shipping_fee")
)

result = (
    ranked
    .select("id", "store_id", "status", "channel", "discount", "shipping_fee")
    .orderBy(F.col("store_id").asc_nulls_last())
)
