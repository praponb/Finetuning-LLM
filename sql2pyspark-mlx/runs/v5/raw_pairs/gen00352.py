from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")

result = (
    df_products
    .withColumn("avg_price_by_category", F.avg("price").over(w))
    .filter(F.col("price") > F.col("avg_price_by_category"))
    .select("id", "category", "price", "launch_date")
)
