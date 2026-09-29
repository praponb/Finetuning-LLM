from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").desc())

result = (
    df_products
    .select("id", "brand", "cost", "product_name", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 2)
    .select("id", "brand", "cost", "product_name")
    .orderBy("brand")
)
