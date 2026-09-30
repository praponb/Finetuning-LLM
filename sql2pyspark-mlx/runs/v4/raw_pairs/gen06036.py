from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .select("id", "channel", "shipping_fee", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 1)
    .select("id", "channel", "shipping_fee")
)
