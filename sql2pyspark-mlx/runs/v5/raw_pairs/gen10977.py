from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([1, 10, 12]) & F.col("cost").between(61, 88) & ~F.col("carrier").isin(["Kerry", "DHL"]))
    .select(
        "delivered_date",
        "shipped_date",
        "order_id",
        F.concat_ws("-", "carrier", "status").alias("carrier_status_key"),
    )
)
