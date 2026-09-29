from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-04-01", "2025-09-01") & ((F.col("status") == "RETURNED") | F.col("carrier").isin(["DHL", "UPS", "FedEx"])))
    .select(
        "carrier",
        "shipped_date",
        "delivered_date",
        F.concat_ws("/", "status", "carrier").alias("status_carrier_key"),
    )
    .orderBy("carrier", "delivered_date")
)
