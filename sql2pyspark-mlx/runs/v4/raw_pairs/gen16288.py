from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["FedEx", "UPS", "Flash"]))
    .select(
        "carrier",
        "shipped_date",
        "order_id",
        F.substring("carrier", 1, 4).alias("carrier_prefix"),
    )
)
