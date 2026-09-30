from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price", "id")
w2 = Window.partitionBy("order_id")

result = (
    df_order_items
    .filter(F.col("order_id") == 25)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.row_number().over(w).alias("event_seq"),
        F.stddev_samp("quantity").over(w2).alias("order_id_stddev_samp_quantity"),
    )
)
