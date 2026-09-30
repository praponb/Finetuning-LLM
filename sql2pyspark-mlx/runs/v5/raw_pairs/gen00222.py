from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_quantity").desc())

result = (
    df_inventory
    .filter(F.col("store_id").isin([4, 5]))
    .groupBy(F.year("last_restock").alias("last_restock_year"))
    .agg(F.sum("quantity").alias("total_quantity"))
    .withColumn("quantity_rank", F.rank().over(w))
    .orderBy(F.col("last_restock_year").desc())
    .limit(10)
)
