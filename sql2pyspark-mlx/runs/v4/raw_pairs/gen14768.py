from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2024-05-01", "2025-07-01"))
    .select("shipped_date", F.lit("shipment").alias("source"))
    .union(
        df_inventory
        .filter(F.col("product_id") == 9)
        .select("last_restock", F.lit("inventory").alias("source"))
    )
    .distinct()
    .orderBy("source")
)
