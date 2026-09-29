from pyspark.sql import functions as F

joined = (
    df_shipments
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.order_date"), F.col("d.shipping_fee"))
)

result = (
    joined
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "order_date",
        "shipping_fee",
        F.when(F.col("status").rlike("^(LOST|RETURNED)$"), "CORE").when(F.col("status").rlike("^(IN_TRANSIT|DELIVERED)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("status_zone"),
    )
    .orderBy("id", F.col("cost").desc())
)
