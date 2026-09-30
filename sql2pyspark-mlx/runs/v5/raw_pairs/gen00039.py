from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["RETURNED", "COMPLETED"]))
    .select(F.col("shipping_fee").alias("value"), F.lit("order").alias("record_type"))
    .unionByName(
        df_shipments
        .filter(F.col("delivered_date").between("2024-08-20", "2025-09-17"))
        .select(F.col("cost").alias("value"), F.lit("shipment").alias("record_type"))
    )
    .distinct()
)
