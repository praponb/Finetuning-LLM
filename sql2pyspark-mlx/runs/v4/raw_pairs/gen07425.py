from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w_running = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("unit_price", "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "quantity",
        "unit_price",
        F.col("quantity").cast("string").alias("quantity_num"),
    )
)

result = (
    cleaned
    .select(
        "*",
        F.first("quantity", ignorenulls=True).over(w_grp).alias("first_known_quantity"),
        F.first("quantity", ignorenulls=True).over(w_grp).alias("first_known_quantity_2"),
        F.avg("unit_price").over(w_recent).alias("unit_price_avg_2"),
        F.last("unit_price", ignorenulls=True).over(w_running).alias("last_known_unit_price"),
        F.lag("unit_price", 2, 0).over(w4).alias("prev_unit_price"),
    )
)
