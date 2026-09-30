from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 9, 10]))
    .select("id", "store_id", "last_restock", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "store_id", "last_restock")
    .orderBy("last_restock")
)
