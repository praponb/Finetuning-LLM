from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .select("id", "brand", "price", "cost", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .select("id", "brand", "price", "cost")
)
