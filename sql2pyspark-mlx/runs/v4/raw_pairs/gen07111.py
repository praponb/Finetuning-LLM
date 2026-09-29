from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units")

result = (
    df_store_sales
    .filter(F.col("revenue") >= 1100)
    .select("id", "product_id", "units", "store_id", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 2)
    .select("id", "product_id", "units", "store_id")
    .orderBy(F.col("units").desc())
)
