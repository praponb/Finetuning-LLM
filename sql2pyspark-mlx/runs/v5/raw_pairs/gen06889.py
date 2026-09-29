from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "RETURNED"]) & F.col("shipped_date").between("2024-05-01", "2025-05-30"))
    .select("id", "delivered_date", "cost", "status")
)
