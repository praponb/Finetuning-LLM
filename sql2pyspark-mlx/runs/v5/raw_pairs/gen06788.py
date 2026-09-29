from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("quantity_num", "id")
w3 = Window.partitionBy("order_id").orderBy("unit_price", "id")
w4 = Window.partitionBy("order_id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.least("quantity", "unit_price").alias("least_quantity_unit_price"),
        F.col("quantity").cast("double").alias("quantity_num"),
    )
)

result = (
    base
    .select(
        "id",
        "quantity",
        "unit_price",
        "quantity_num",
        F.ntile(3).over(w).alias("unit_price_bucket"),
        F.row_number().over(w2).alias("event_seq"),
        F.lead("unit_price", 2, 0).over(w3).alias("next_unit_price"),
        F.lead("unit_price", 1, 0).over(w2).alias("next_unit_price_2"),
        F.stddev_samp("quantity_num").over(w4).alias("order_id_stddev_samp_quantity_num"),
    )
)
