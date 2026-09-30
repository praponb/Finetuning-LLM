from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("quantity").desc())

result = (
    df_order_items
    .filter(F.col("quantity") >= 6)
    .select(
        "id",
        "unit_price",
        "order_id",
        "quantity",
        F.row_number().over(w).alias("row_num"),
        F.rank().over(w2).alias("quantity_rank"),
    )
)
