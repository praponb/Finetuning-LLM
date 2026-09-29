from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2024-09-01", "2025-05-01"))
    .select(
        F.lit("shipment").alias("record_type"),
        F.col("shipped_date").alias("event_date"),
    )
    .unionByName(
        df_customers
        .filter(F.col("age") <= 31)
        .select(
        F.lit("customer").alias("record_type"),
        F.col("signup_date").alias("event_date"),
    )
    )
    .distinct()
)
