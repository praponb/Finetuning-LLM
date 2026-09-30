from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock_month")

result = (
    df_inventory
    .filter(F.col("product_id").isin([6, 8, 10]))
    .groupBy(F.month("last_restock").alias("last_restock_month"))
    .agg(F.sum("quantity").alias("total_quantity"))
    .withColumn("cumulative_quantity", F.sum("total_quantity").over(w))
)
