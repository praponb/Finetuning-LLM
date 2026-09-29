from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.orderBy(F.col("unit_price").desc(), "id")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "unit_price",
        F.min("unit_price").over(w).alias("product_id_min_unit_price"),
        F.ntile(2).over(w2).alias("bucket"),
    )
)
