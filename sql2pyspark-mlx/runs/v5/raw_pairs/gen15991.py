from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("stock_qty").desc())

result = (
    df_products
    .select(
        "id",
        "category",
        "stock_qty",
        "cost",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "category", "stock_qty", "cost")
    .orderBy(F.col("stock_qty").desc(), F.col("id").desc())
)
