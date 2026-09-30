from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")
w4 = Window.orderBy("quantity", "id").rowsBetween(-2, -1)

result = (
    df_order_items
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.corr("quantity", "unit_price").over(w2).alias("corr_quantity_unit_price"),
        F.row_number().over(w3).alias("seq_no"),
        F.avg("unit_price").over(w4).alias("unit_price_avg_2_prior"),
    )
)
