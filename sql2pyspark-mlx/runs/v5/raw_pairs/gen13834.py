from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2024-03-01", "2025-04-03") & F.col("order_id").isNotNull())
    .select(
        "delivered_date",
        "carrier",
        "cost",
        F.lower("status").alias("status_lower"),
    )
)
