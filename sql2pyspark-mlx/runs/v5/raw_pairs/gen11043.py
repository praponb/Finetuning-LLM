from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(-2, Window.currentRow)
w_ordered_2 = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        F.count("unit_price").over(w_ordered).alias("last3_count_unit_price"),
        F.avg("unit_price").over(w_ordered).alias("last3_avg_unit_price"),
        F.max("unit_price").over(w_ordered).alias("last3_max_unit_price"),
        F.row_number().over(w_ordered_2).alias("event_seq"),
    )
)
