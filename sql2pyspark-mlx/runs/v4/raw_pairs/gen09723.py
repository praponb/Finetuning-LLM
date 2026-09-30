from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock")

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 3, 6, 8]))
    .select("id", "product_id", "last_restock", F.rank().over(w).alias("rnk"))
)
