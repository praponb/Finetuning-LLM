from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("quantity", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(-5, -1)
w5 = Window.orderBy("quantity", "id")

with_windows = (
    df_order_items
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("first_known_unit_price", F.first("unit_price", ignorenulls=True).over(w2))
    .withColumn("unit_price_avg_2_prior", F.avg("unit_price").over(w3))
    .withColumn("unit_price_avg_5_prior", F.avg("unit_price").over(w4))
    .withColumn("quantity_bucket", F.ntile(3).over(w5))
)

result = (
    with_windows
    .select(
        "id",
        "row_seq",
        "first_known_unit_price",
        "unit_price_avg_5_prior",
        "quantity_bucket",
    )
    .orderBy(F.col("quantity_bucket").desc_nulls_last())
)
