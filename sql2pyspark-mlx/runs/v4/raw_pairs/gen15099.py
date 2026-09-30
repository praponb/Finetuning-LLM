from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") != "RETURNED")
    .select(
        "order_id",
        "carrier",
        "status",
        F.col("cost").cast("int").alias("cost_int"),
        F.when(F.col("carrier").isin(["Flash", "FedEx"]), "Priority").otherwise("Other").alias("carrier_group"),
    )
    .orderBy("status")
)
