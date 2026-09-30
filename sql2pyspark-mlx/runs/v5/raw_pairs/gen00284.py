from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("price", "id")

result = (
    df_products
    .select("id", "category", "price", F.row_number().over(w).alias("price_row"))
    .orderBy(F.col("price").desc())
)
