from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter((F.col("delivered_date") > F.to_date(F.lit("2023-06-23"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.concat("carrier", F.lit("/"), "status").alias("carrier_status"),
        F.coalesce("carrier", F.lit("OTHER")).alias("resolved_carrier"),
    )
)

result = (
    cleaned
    .alias("e")
    .join(df_orders.alias("dim"), F.col("e.order_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["channel", "shipment_status"], ["channel"]],
        F.col("dim.channel").alias("channel"),
        F.col("e.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("e.cost")).alias("min_cost"),
    )
)
