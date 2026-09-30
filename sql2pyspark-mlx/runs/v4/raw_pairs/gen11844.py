from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .filter(F.col("product_id").isin([5, 6, 8]))
    .select("id", "product_id", "quantity", F.dense_rank().over(w).alias("dense_rnk"))
)
