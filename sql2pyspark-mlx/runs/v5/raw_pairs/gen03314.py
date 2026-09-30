from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date")

result = (
    df_products
    .select(
        "id",
        "category",
        "launch_date",
        "stock_qty",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 2)
)
