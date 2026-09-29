from pyspark.sql import functions as F

result = (
    df_shipments
    .select("status", "carrier")
    .intersectAll(
        df_shipments
        .filter(F.col("carrier").isin(["DHL", "FedEx"]))
        .select("status", "carrier")
    )
)
