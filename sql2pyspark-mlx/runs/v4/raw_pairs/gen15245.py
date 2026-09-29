from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w_recent = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_last())
w3 = Window.partitionBy("order_id").orderBy("quantity", "id")
w4 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .select(
        "*",
        F.corr("quantity", "unit_price").over(w).alias("corr_quantity_unit_price"),
        F.cume_dist().over(w_recent).alias("quantity_cume_dist"),
        F.ntile(3).over(w3).alias("quantity_bucket"),
        F.sum("quantity").over(w4).alias("cumulative_quantity"),
        F.lead("quantity", 1).over(w5).alias("next_quantity"),
    )
)
