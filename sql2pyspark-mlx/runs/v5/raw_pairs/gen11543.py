from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("stock_qty").desc(), "id")

result = (
    df_products
    .select(
        "id",
        "category",
        "stock_qty",
        "price",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "category", "stock_qty", "price")
    .orderBy("price", F.col("stock_qty").desc())
)
