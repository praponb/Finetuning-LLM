from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())

valid = (
    df_orders
    .filter((F.col("order_date") > F.to_date(F.lit("2023-11-12"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.greatest("shipping_fee", "amount").alias("greatest_shipping_fee_amount"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.delivered_date").alias("shipment_delivered_date"))
)
