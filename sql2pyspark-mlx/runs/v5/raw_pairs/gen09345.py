from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.corr("quantity", "unit_price").over(w).alias("corr_quantity_unit_price"),
        F.lag("quantity", 2).over(w2).alias("prev_quantity"),
    )
)
