from pyspark.sql import functions as F

result = (
    df_shipments
    .select("id", "carrier")
    .intersectAll(
        df_shipments
        .filter(F.col("carrier").isin(["UPS", "DHL", "Flash"]))
        .select("id", "carrier")
    )
)
