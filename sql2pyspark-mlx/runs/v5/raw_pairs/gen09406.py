from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-07-17").cast("date")) & (F.col("status") == "LOST"))
    .select(
        "*",
        F.concat_ws("/", "status", "carrier").alias("status_carrier_key"),
        F.coalesce("carrier", F.lit("OTHER")).alias("resolved_carrier"),
    )
)

result = (
    cleaned
    .alias("src")
    .join(df_orders.alias("dim"), F.col("src.order_id") == F.col("dim.id"), "inner")
    .groupBy(
        F.col("dim.channel").alias("channel"),
        F.col("dim.status").alias("order_status"),
        F.col("src.carrier").alias("carrier"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("src.cost")), 2).alias("avg_cost"),
    )
)
