from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipping_fee")

result = (
    df_orders
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "status", "shipping_fee", "discount")
    .orderBy(F.col("discount").desc())
)
