from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_order_items
    .filter(F.col("order_id").isin([18, 25, 29, 35]))
    .select(
        "id",
        "unit_price",
        "quantity",
        "product_id",
        F.lead("quantity", 1, 0).over(w).alias("next_quantity"),
        F.last("unit_price").over(w2).alias("last_unit_price"),
    )
    .orderBy(F.col("unit_price").desc())
)
