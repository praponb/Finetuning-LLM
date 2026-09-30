from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("amount")

result = (
    df_orders
    .select("id", "store_id", "amount", F.dense_rank().over(w).alias("dense_rnk"))
    .orderBy(F.col("dense_rnk").desc())
)
