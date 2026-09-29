from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2024-10-01", "2025-01-01") | (F.col("status") == "LOST"))
    .select("id", "status", "delivered_date", "shipped_date")
)
