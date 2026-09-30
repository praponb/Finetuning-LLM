from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock", F.lit("inventory").alias("source"))
    .union(
        df_shipments
        .filter(F.col("delivered_date") <= "2024-08-10")
        .select("delivered_date", F.lit("shipment").alias("source"))
    )
)
