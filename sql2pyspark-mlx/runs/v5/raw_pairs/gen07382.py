from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_order_items
    .filter(F.col("order_id").isin([7, 16]))
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "product_id",
        F.last("unit_price").over(w).alias("last_unit_price"),
        F.lead("quantity", 2).over(w2).alias("next_quantity"),
    )
)
