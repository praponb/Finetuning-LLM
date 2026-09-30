from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_orders
    .filter(F.col("channel").isin(["partner", "web", "store"]))
    .groupBy("channel")
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
    .withColumn("pct_of_total", F.round(((F.col("total_shipping_fee") * 100) / F.sum("total_shipping_fee").over(w)), 2))
)
