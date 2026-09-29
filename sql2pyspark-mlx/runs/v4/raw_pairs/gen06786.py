from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "order_id",
        "quantity",
        F.row_number().over(w).alias("row_num"),
    )
    .orderBy("row_num", F.col("product_id").desc())
)
