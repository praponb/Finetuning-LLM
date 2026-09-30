from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["DHL", "Flash", "FedEx"]))
    .select("shipped_date", "status")
    .unionByName(
        df_shipments
        .filter(F.col("carrier") != "FedEx")
        .select("shipped_date", "status")
    )
    .distinct()
    .orderBy(F.col("status").desc())
)
