from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")

result = (
    df_products
    .withColumn("max_price_by_category", F.max("price").over(w))
    .filter(F.col("price") == F.col("max_price_by_category"))
    .select("id", "category", "price")
)
