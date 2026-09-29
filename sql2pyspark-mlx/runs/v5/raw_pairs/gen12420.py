from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["North", "Central"]))
    .select(F.lit("store").alias("record_type"), "opened_date")
    .union(
        df_shipments
        .filter(F.col("cost").between(37, 48))
        .select(F.lit("shipment").alias("record_type"), "delivered_date")
    )
    .distinct()
)
