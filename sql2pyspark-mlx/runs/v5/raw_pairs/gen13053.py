from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy("price", "id")

result = (
    df_products
    .filter(F.col("stock_qty") != 311)
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        F.first("cost").over(w).alias("first_cost"),
    )
)
