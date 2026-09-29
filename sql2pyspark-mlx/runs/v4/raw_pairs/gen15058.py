from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_quantity").desc())

result = (
    df_inventory
    .filter(F.col("product_id") == 5)
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("last_restock_month"))
    .agg(F.sum("quantity").alias("total_quantity"))
    .withColumn("rnk", F.dense_rank().over(w))
)
