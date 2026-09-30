from pyspark.sql import functions as F

result = (
    df_shipments
    .select("delivered_date", F.lit("shipment").alias("source"))
    .unionByName(
        df_inventory
        .select(
        F.col("last_restock").alias("delivered_date"),
        F.lit("inventory").alias("source"),
    )
    )
    .distinct()
    .orderBy("delivered_date")
)
