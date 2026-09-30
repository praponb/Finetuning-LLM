from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")

result = (
    df_products
    .select("id", "category", "cost", F.min("cost").over(w).alias("category_min_cost"))
)
