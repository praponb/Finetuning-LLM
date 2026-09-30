from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("price")
w2 = Window.orderBy(F.col("cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_products
    .select(
        "id",
        "category",
        "price",
        "cost",
        "product_name",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.last("product_name").over(w2).alias("last_product_name"),
    )
)
