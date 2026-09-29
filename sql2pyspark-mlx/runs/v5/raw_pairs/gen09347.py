from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.orderBy("quantity", "id")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "order_id",
        "quantity",
        "unit_price",
        F.first("unit_price").over(w).alias("first_unit_price"),
        F.row_number().over(w2).alias("quantity_row"),
    )
)
