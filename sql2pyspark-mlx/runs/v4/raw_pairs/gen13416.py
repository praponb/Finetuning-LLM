from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("shipping_fee")

result = (
    df_orders
    .select("id", "store_id", "shipping_fee", F.dense_rank().over(w).alias("dense_rnk"))
)
