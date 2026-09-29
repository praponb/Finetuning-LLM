from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("stock_qty")
w2 = Window.partitionBy("category").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "stock_qty",
        "category",
        "launch_date",
        "price",
        F.rank().over(w).alias("stock_qty_rank"),
        F.lead("price", 2).over(w2).alias("next_price"),
    )
)
