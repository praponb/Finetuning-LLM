from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "product_id",
        F.row_number().over(w).alias("rn"),
        F.ntile(10).over(w2).alias("quantity_ntile"),
    )
)
