from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("cost").desc())

result = (
    df_products
    .filter(F.array_contains("tags", "bestseller"))
    .select("id", "category", "cost", F.rank().over(w).alias("rnk"))
)
