from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w_part = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc())
w3 = Window.partitionBy("product_id")

scored = (
    df_order_items
    .filter(F.col("unit_price").between(180, 220))
    .withColumn("order_id_stddev_samp_quantity", F.stddev_samp("quantity").over(w))
    .withColumn("unit_price_percent_rank", F.percent_rank().over(w_part))
    .withColumn("corr_quantity_unit_price", F.corr("quantity", "unit_price").over(w3))
)

result = (
    scored
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "order_id_stddev_samp_quantity",
        "unit_price_percent_rank",
        "corr_quantity_unit_price",
    )
    .orderBy(F.col("product_id").asc_nulls_first())
)
