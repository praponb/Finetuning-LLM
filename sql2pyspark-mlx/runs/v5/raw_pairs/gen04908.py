from pyspark.sql import functions as F

result = (
    df_transactions
    .select(F.lit("transaction").alias("source"), "amount")
    .union(
        df_shipments
        .filter(F.col("status") == "RETURNED")
        .select(F.lit("shipment").alias("source"), "cost")
    )
    .distinct()
    .orderBy("amount")
)
