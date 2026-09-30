from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy("cost", "id").rowsBetween(-3, Window.currentRow)

valid = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .withColumn("carrier_length", F.length("carrier"))
)

joined = (
    valid
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.order_date"), F.col("d.discount"))
)

enriched = (
    joined
    .select(
        "*",
        F.max("carrier_length").over(w).alias("last4_max_carrier_length"),
        F.sum("carrier_length").over(w).alias("last4_sum_carrier_length"),
        F.min("carrier_length").over(w).alias("last4_min_carrier_length"),
    )
)

joined_shipments = (
    enriched
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.amount") > 1350)
    .select(
        "s.id",
        "s.order_id",
        "s.carrier",
        "s.delivered_date",
        "s.order_date",
        "s.discount",
        "s.last4_sum_carrier_length",
        F.col("b.status").alias("order_status"),
        F.col("b.shipping_fee"),
        F.col("b.order_date").alias("order_order_date"),
    )
)

result = (
    joined_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        "last4_sum_carrier_length",
        "order_status",
        F.when(F.col("carrier").rlike("^(DHL|FedEx)$"), "GROUP_A").when(F.col("carrier").rlike("^(Kerry|Flash)$"), "GROUP_B").otherwise("CORE").alias("carrier_zone"),
    )
)
