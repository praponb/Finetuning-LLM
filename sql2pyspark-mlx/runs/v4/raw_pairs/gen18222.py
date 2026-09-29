from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

with_latest = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"), F.col("c.unit_price"))
)

result = (
    with_latest
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        "shipping_fee",
        "quantity",
        F.when(F.col("channel").rlike("^(store|mobile)$"), "PRIORITY").otherwise("GROUP_B").alias("channel_zone"),
    )
    .orderBy(F.col("quantity").desc(), "customer_id")
)
