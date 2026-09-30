from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc())

result = (
    df_orders
    .select("id", "customer_id", "shipping_fee", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .orderBy("pos", F.col("id").desc())
)
