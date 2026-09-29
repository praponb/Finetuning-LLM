from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .select("id", "category", "price", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .select("id", "category", "price")
    .orderBy("category")
)
