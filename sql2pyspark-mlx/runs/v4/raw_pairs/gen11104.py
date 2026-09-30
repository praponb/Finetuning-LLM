from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & ~F.col("carrier").isin(["FedEx", "DHL", "UPS"]))
    .withColumn("carrier_prefix", F.substring("carrier", 1, 2))
)

result = (
    filtered
    .select("carrier")
    .intersect(
        df_shipments
        .filter(F.col("cost") < 32)
        .select("carrier")
    )
)
