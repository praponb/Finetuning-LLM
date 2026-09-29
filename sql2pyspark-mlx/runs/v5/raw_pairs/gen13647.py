from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date", "id")
w2 = Window.partitionBy("brand")

result = (
    df_products
    .select(
        "id",
        "brand",
        "launch_date",
        "stock_qty",
        "price",
        (F.col("stock_qty") - F.lag("stock_qty", 2).over(w)).alias("stock_qty_change"),
        F.round((F.col("price") / F.sum("price").over(w2)), 3).alias("price_share"),
    )
)
