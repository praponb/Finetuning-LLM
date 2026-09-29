from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("store_id")

result = (
    df_inventory
    .filter(F.year("last_restock") >= 2023)
    .groupBy("store_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_quantity", F.sum("total_quantity").over(w))
    .orderBy(F.col("store_id").desc())
)
