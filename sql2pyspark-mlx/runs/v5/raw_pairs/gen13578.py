from pyspark.sql import functions as F

valid = (
    df_shipments
    .filter(F.col("delivered_date") > F.lit("2023-07-03").cast("date"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.length("status").alias("status_length"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_clean"),
    )
)

result = (
    valid
    .groupBy("order_id")
    .pivot("carrier", ["DHL", "UPS", "FedEx"])
    .agg(F.avg("cost"))
    .select(
        "order_id",
        F.col("DHL").alias("dhl"),
        F.col("UPS").alias("ups"),
        F.col("FedEx").alias("fedex"),
    )
)
