from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .select("id", "store_id", "last_restock", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
    .orderBy("last_restock")
)
