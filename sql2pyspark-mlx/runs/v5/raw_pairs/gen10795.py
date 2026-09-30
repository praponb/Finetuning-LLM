from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("order_id")

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity",
        F.last("quantity").over(w).alias("last_quantity"),
        F.min("quantity").over(w2).alias("order_id_min_quantity"),
    )
    .orderBy(F.col("quantity").desc())
)
