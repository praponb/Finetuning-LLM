from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["FedEx", "DHL", "Flash"]))
    .select("shipped_date", "carrier")
    .orderBy(F.col("carrier").desc(), "shipped_date")
)
