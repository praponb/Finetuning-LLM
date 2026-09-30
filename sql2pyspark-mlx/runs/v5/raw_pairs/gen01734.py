from pyspark.sql import functions as F

result = (
    df_shipments
    .select("cost", F.lit("shipment").alias("kind"))
    .unionByName(
        df_transactions
        .filter(F.col("is_flagged"))
        .select(F.col("amount").alias("cost"), F.lit("transaction").alias("kind"))
    )
)
