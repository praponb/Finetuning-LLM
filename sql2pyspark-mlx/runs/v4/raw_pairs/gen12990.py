from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["Flash", "FedEx", "DHL"]) & (F.col("cost") < 39))
    .select("id", "carrier", "shipped_date", "status")
)
