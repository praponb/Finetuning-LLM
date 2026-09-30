from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

with_last = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.unit_price").alias("order_item_unit_price"),
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

result = (
    with_last
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        "order_item_unit_price",
        "order_item_quantity",
    )
    .orderBy(F.col("shipping_fee").desc_nulls_last())
)
