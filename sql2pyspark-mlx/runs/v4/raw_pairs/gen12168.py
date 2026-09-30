from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock")

result = (
    df_inventory
    .select("id", "store_id", "last_restock", F.dense_rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .select("id", "store_id", "last_restock")
    .orderBy(F.col("store_id").desc())
)
