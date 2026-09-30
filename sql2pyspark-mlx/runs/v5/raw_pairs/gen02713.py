from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")

result = (
    df_order_items
    .filter(F.col("unit_price").between(100, 270))
    .select(
        "id",
        "order_id",
        "unit_price",
        "product_id",
        F.first("product_id").over(w).alias("first_product_id"),
    )
)
