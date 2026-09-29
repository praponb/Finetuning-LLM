from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

valid = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-08-18").cast("date")) & (F.col("status") != "SHIPPED"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.when(F.col("amount") >= 1270, "High").when(F.col("amount") >= 230, "Medium").otherwise("Low").alias("amount_tier"),
        F.greatest("amount", "shipping_fee").alias("greatest_amount_shipping_fee"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.carrier").alias("shipment_carrier"),
        F.col("c.delivered_date").alias("shipment_delivered_date"),
    )
)
