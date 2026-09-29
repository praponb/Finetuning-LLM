from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("product_id")
w3 = Window.orderBy(F.col("quantity").desc(), "id")
w4 = Window.orderBy("unit_price", "id")

result = (
    df_order_items
    .filter(F.col("unit_price") >= 170)
    .select(
        "id",
        "product_id",
        "unit_price",
        F.max("quantity").over(w).alias("order_id_max_quantity"),
        F.corr("unit_price", "quantity").over(w2).alias("corr_unit_price_quantity"),
        F.row_number().over(w3).alias("seq_no"),
        F.avg("quantity").over(w).alias("order_id_avg_quantity"),
        F.row_number().over(w4).alias("event_seq"),
    )
)
