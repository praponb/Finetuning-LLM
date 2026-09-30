from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")
w4 = Window.partitionBy("product_id", "order_id")
w_ordered = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .select(
        "*",
        F.avg("unit_price").over(w).alias("unit_price_avg_6_prior"),
        F.lead("quantity", 1).over(w2).alias("next_quantity"),
        F.ntile(5).over(w3).alias("unit_price_bucket"),
        F.avg("quantity").over(w4).alias("product_id_avg_quantity"),
        F.sum("unit_price").over(w_ordered).alias("running_unit_price"),
    )
)
