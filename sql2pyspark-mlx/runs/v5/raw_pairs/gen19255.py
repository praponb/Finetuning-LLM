from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.col("cost").cast("int").alias("cost_int"),
        F.last_day("delivered_date").alias("delivered_date_month_end"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_clean"),
    )
)

result = (
    cleaned
    .groupBy("carrier", "order_id")
    .pivot("status", ["IN_TRANSIT", "LOST", "RETURNED"])
    .agg(F.avg("cost_int").alias("total"))
    .select(
        "carrier",
        "order_id",
        F.col("IN_TRANSIT").alias("in_transit"),
        F.col("LOST").alias("lost"),
        F.col("RETURNED").alias("returned"),
    )
)
