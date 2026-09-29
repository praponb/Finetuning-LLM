from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc())

result = (
    df_products
    .filter(F.size("tags") >= 1)
    .select("id", "supplier_id", "stock_qty", F.dense_rank().over(w).alias("dense_rnk"))
)
