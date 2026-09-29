from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-12-15").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        F.concat_ws("-", "carrier", "status").alias("carrier_status_key"),
        F.concat("status", F.lit(" - "), "carrier").alias("status_carrier"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_clean"),
    )
)

result = (
    filtered
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.amount"))
)
