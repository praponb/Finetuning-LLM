from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") < "2025-02-01")
    .select(F.lit("shipment").alias("source"), "delivered_date")
    .union(
        df_stores
        .filter(F.col("opened_date") > "2024-09-01")
        .select(F.lit("store").alias("source"), "opened_date")
    )
    .distinct()
    .orderBy("source")
)
