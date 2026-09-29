from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("cost")
w2 = Window.partitionBy("category").orderBy("price")

result = (
    df_products
    .select(
        "id",
        "category",
        "cost",
        "price",
        F.rank().over(w).alias("rnk"),
        F.dense_rank().over(w2).alias("price_dense_rank"),
    )
)
