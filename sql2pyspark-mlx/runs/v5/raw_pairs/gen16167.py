from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 5, 9]))
    .select("id", "store_id", "last_restock", F.row_number().over(w).alias("rn"))
)
