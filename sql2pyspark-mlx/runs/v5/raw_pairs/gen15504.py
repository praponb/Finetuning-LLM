from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "status", "shipping_fee")
    .orderBy(F.col("shipping_fee").desc())
)
