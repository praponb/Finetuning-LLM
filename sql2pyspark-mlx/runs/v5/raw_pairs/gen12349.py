from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.lit("shipment").alias("origin"), "delivered_date")
    .union(
        df_inventory
        .select(F.lit("inventory").alias("origin"), "last_restock")
    )
    .distinct()
)
