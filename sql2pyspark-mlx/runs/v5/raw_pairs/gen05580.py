from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id")
w2 = Window.orderBy(F.col("cost").desc())

result = (
    df_products
    .select(
        "id",
        "category",
        "launch_date",
        "stock_qty",
        "cost",
        F.lead("stock_qty", 1).over(w).alias("next_stock_qty"),
        F.dense_rank().over(w2).alias("cost_dense_rank"),
    )
)
