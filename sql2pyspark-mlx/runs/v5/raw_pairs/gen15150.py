from pyspark.sql import functions as F

result = (
    df_payments
    .select(F.lit("payment").alias("source"), "status")
    .unionByName(
        df_shipments
        .filter(F.col("carrier") == "DHL")
        .select(F.lit("shipment").alias("source"), "status")
    )
    .orderBy("status")
)
