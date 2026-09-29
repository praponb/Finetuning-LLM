from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy("quantity", "id")
w3 = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").desc_nulls_first())

result = (
    df_order_items
    .select(
        "*",
        F.avg("quantity").over(w).alias("quantity_avg_2"),
        F.row_number().over(w2).alias("seq_no"),
        F.cume_dist().over(w3).alias("quantity_cume_dist"),
    )
)
