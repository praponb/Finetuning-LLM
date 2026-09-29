from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([8, 9, 12]))
    .select("delivered_date", "shipped_date")
    .unionByName(
        df_shipments
        .filter(~F.col("carrier").isin(["Kerry", "FedEx"]))
        .select("delivered_date", "shipped_date")
    )
    .distinct()
)
