from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc())

result = (
    df_products
    .select(
        "id",
        "brand",
        "stock_qty",
        "launch_date",
        F.dense_rank().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") == 1)
    .select("id", "brand", "stock_qty", "launch_date")
    .orderBy("brand")
)
