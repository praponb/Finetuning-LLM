from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .filter(F.col("amount").isNotNull())
    .select("id", "channel", "shipping_fee", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 2)
)
