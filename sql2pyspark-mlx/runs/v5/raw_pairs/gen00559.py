from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_date", "id")
w2 = Window.orderBy("stock_qty", "id")
w3 = Window.partitionBy("category").orderBy("stock_qty", "id")

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "price",
        "tags",
        F.lag("price", 1).over(w).alias("prev_price"),
        F.ntile(5).over(w2).alias("stock_qty_bucket"),
        F.ntile(4).over(w3).alias("stock_qty_bucket_2"),
    )
)
