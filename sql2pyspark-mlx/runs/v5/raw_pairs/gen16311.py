from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_order_items
    .select(
        "id",
        "quantity",
        "product_id",
        F.last("product_id").over(w).alias("last_product_id"),
    )
)
