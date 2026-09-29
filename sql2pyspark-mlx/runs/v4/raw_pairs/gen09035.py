from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity")

result = (
    df_inventory
    .select("id", "product_id", "quantity", F.dense_rank().over(w).alias("dense_rnk"))
    .orderBy(F.col("dense_rnk").desc())
)
