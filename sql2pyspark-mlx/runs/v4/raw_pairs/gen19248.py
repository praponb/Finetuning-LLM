from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("unit_price")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "product_id",
        "unit_price",
        F.first("product_id").over(w).alias("first_product_id"),
        F.rank().over(w2).alias("unit_price_rank"),
    )
)
