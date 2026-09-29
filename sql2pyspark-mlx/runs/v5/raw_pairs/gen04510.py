from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc())

result = (
    df_inventory
    .select("id", "store_id", "last_restock", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .select("id", "store_id", "last_restock")
)
