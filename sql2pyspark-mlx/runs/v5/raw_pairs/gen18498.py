from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price", "id")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id")
w3 = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        F.lead("quantity", 1, 0).over(w).alias("next_quantity"),
        F.row_number().over(w2).alias("seq_no"),
        F.first("unit_price", ignorenulls=True).over(w3).alias("first_known_unit_price"),
        F.round((F.col("quantity") / F.sum("quantity").over(w4)), 4).alias("quantity_share"),
    )
)
