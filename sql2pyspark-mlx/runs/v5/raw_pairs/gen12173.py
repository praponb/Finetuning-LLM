from pyspark.sql import functions as F

result = (
    df_transactions
    .select("amount", F.lit("transaction").alias("kind"))
    .union(
        df_shipments
        .filter(F.col("status") == "DELIVERED")
        .select("cost", F.lit("shipment").alias("kind"))
    )
    .distinct()
)
