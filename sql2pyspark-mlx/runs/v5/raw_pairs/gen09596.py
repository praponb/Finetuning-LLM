from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_order_items
    .filter(F.col("order_id").isin([16, 29]))
    .select(
        "id",
        "product_id",
        "order_id",
        "quantity",
        "unit_price",
        F.row_number().over(w).alias("row_num"),
        F.lag("unit_price", 1, 0).over(w2).alias("prev_unit_price"),
    )
    .orderBy(F.col("id").desc())
)
