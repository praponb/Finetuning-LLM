from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")

enriched = (
    df_order_items
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.sum("unit_price").over(w).alias("running_unit_price"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "running_unit_price",
        "row_seq",
    )
    .orderBy("unit_price")
)
