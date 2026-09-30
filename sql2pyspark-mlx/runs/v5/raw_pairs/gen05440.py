from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_order_items
    .filter(F.col("unit_price").between(110, 310))
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 2)
    .select("id", "order_id", "quantity", "unit_price")
    .orderBy(F.col("quantity").desc())
)
