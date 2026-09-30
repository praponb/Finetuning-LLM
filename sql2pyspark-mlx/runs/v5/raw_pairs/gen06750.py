from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 5]))
    .select("id", "product_id", "quantity", F.dense_rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 5)
    .select("id", "product_id", "quantity")
    .orderBy(F.col("quantity").desc(), "product_id")
)
