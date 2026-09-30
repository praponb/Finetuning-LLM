from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("launch_date").desc(), "id")
w2 = Window.partitionBy("brand").orderBy("cost")

result = (
    df_products
    .filter(F.col("category").isin(["Books", "Sports"]))
    .select(
        "id",
        "launch_date",
        "brand",
        "cost",
        F.row_number().over(w).alias("launch_date_row"),
        F.dense_rank().over(w2).alias("cost_dense_rank"),
    )
)
