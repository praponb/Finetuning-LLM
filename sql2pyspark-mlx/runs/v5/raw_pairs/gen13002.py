from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 16)
    .select("id", "carrier", "order_id")
    .unionByName(
        df_shipments
        .filter(F.col("carrier").isin(["DHL", "FedEx"]))
        .select("id", "carrier", "order_id")
    )
    .distinct()
)
