from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 3]))
    .select("id", "store_id", "last_restock", F.ntile(3).over(w).alias("quartile"))
)
