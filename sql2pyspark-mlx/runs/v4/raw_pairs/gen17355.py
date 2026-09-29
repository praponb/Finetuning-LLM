from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")
w3 = Window.partitionBy("product_id")
w4 = Window.orderBy(F.col("unit_price").desc_nulls_first())

enriched = (
    df_order_items
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w))
    .withColumn("prev_unit_price", F.lag("unit_price", 2, 0).over(w2))
    .withColumn("product_id_avg_unit_price", F.avg("unit_price").over(w3))
    .withColumn("unit_price_percent_rank", F.percent_rank().over(w4))
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "first_known_quantity",
        "prev_unit_price",
        "unit_price_percent_rank",
    )
)
