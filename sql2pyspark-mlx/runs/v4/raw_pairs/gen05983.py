from pyspark.sql import functions as F

result = (
    df_orders
    .select("amount", F.lit("order").alias("source"))
    .union(
        df_shipments
        .filter(F.col("status") == "DELIVERED")
        .select("cost", F.lit("shipment").alias("source"))
    )
    .distinct()
)
