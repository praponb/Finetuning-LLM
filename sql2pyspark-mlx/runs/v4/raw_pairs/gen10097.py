from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id").rowsBetween(-5, -1)
w_running = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("product_id")
w4 = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_order_items
    .withColumn("quantity_avg_5_prior", F.avg("quantity").over(w))
    .withColumn("quantity_avg_2", F.avg("quantity").over(w_running))
    .withColumn("product_id_stddev_samp_unit_price", F.stddev_samp("unit_price").over(w3))
    .withColumn("running_unit_price", F.sum("unit_price").over(w4))
)

result = (
    with_windows
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "quantity_avg_2",
        "running_unit_price",
    )
    .orderBy(F.col("order_id").desc(), F.col("quantity_avg_2").desc())
)
