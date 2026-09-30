from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter(F.col("carrier") == "FedEx")
    .select(
        "*",
        F.date_format("delivered_date", "MMM yyyy").alias("delivered_date_label"),
        F.coalesce("carrier", F.lit("N/A")).alias("resolved_carrier"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_orders.alias("dim"), F.col("s.order_id") == F.col("dim.id"), "inner")
    .rollup(
        F.col("dim.channel").alias("channel"),
        F.col("s.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("s.cost")), 2).alias("avg_cost"),
    )
    .orderBy(F.col("avg_cost").desc())
)
