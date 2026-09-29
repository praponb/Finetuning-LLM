from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-07-30").cast("date")) & F.col("carrier").isNotNull())
    .withColumn("status_prefix", F.substring("status", 1, 3))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("UNKNOWN")))
)

result = (
    filtered
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.amount") < 600)
    .select(
        "e.id",
        "e.order_id",
        "e.carrier",
        "e.delivered_date",
        "e.cost",
        "e.carrier_filled",
        F.col("o.status").alias("order_status"),
    )
)
