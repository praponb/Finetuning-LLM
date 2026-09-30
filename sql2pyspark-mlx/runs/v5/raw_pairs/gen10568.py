from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .filter(F.col("last_restock") < "2024-12-01")
    .select("id", "store_id", "quantity", F.rank().over(w).alias("quantity_rank"))
)
