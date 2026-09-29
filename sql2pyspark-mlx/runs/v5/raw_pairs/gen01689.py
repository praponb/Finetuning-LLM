from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("price").desc(), "id")
w2 = Window.partitionBy("category").orderBy("stock_qty", "id")
w3 = Window.partitionBy("brand")
w4 = Window.partitionBy("supplier_id").orderBy("stock_qty", "id")
w5 = Window.orderBy(F.col("price").desc(), "id")

scored = (
    df_products
    .withColumn("price_bucket", F.ntile(5).over(w))
    .withColumn("stock_qty_bucket", F.ntile(4).over(w2))
    .withColumn("corr_stock_qty_price", F.corr("stock_qty", "price").over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
)

result = (
    scored
    .select(
        "id",
        "category",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "price_bucket",
        "stock_qty_bucket",
        "corr_stock_qty_price",
    )
)
