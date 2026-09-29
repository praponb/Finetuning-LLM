from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "DHL"]))
    .select(F.col("cost").alias("value"), F.lit("shipment").alias("kind"))
    .unionByName(
        df_transactions
        .select(F.col("amount").alias("value"), F.lit("transaction").alias("kind"))
    )
    .orderBy(F.col("kind").desc())
)
