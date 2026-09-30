from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("order_date").desc())

result = (
    df_orders
    .filter(~F.col("channel").isin(["web", "partner", "store"]))
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 1)
    .orderBy(F.col("customer_id").desc())
)
