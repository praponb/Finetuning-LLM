from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("price").desc())

result = (
    df_products
    .select("id", "supplier_id", "price", F.rank().over(w).alias("rnk"))
    .orderBy(F.col("supplier_id").desc())
)
