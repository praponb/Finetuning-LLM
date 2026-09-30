from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["Kerry", "DHL", "UPS"]))
    .select("id", "status", "carrier")
)
