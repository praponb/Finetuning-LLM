from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price")

result = (
    df_order_items
    .filter(F.col("quantity") <= 4)
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity",
        F.rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "order_id", "unit_price", "quantity")
    .orderBy(F.col("quantity").desc())
)
