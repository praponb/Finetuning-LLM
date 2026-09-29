from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-03-01", "2024-01-01"))
    .select("id", "store_id", "quantity", F.dense_rank().over(w).alias("dense_rnk"))
)
