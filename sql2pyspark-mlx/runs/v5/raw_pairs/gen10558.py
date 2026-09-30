from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price", "id")
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .filter(F.col("quantity").between(5, 8))
    .select(
        "id",
        "unit_price",
        "order_id",
        F.row_number().over(w).alias("unit_price_row"),
        (F.col("unit_price") - F.lag("unit_price", 1).over(w2)).alias("unit_price_change"),
    )
    .orderBy("id", F.col("unit_price_change").desc())
)
