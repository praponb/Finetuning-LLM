from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("unit_price").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id")
w3 = Window.partitionBy("order_id").orderBy("unit_price", "id")
w_ordered = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") >= 270))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
    )
)

result = (
    cleaned
    .select(
        "*",
        F.ntile(10).over(w_grp).alias("unit_price_bucket"),
        F.lead("unit_price", 1, 0).over(w2).alias("next_unit_price"),
        F.ntile(10).over(w3).alias("unit_price_bucket_2"),
        F.first("unit_price", ignorenulls=True).over(w_ordered).alias("first_known_unit_price"),
        F.ntile(4).over(w_grp).alias("unit_price_bucket_3"),
    )
)
