from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock")

result = (
    df_inventory
    .filter(F.col("last_restock") > "2024-04-01")
    .select("id", "store_id", "last_restock", F.dense_rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
)
