from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("store_id")

result = (
    df_inventory
    .groupBy("store_id")
    .agg(F.sum("quantity").alias("total_quantity"))
    .withColumn("cumulative_quantity", F.sum("total_quantity").over(w))
    .orderBy("store_id", F.col("cumulative_quantity").desc())
)
