from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_unit_price").desc())

result = (
    df_order_items
    .filter(F.col("order_id").isin([16, 26, 35]))
    .groupBy("order_id")
    .agg(F.sum("unit_price").alias("total_unit_price"))
    .withColumn("unit_price_rank", F.rank().over(w))
    .orderBy("order_id")
)
