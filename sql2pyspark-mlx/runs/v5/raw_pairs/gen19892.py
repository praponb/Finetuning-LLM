from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2024-09-01", "2025-03-02") & (F.col("status") == "IN_TRANSIT") & F.col("status").isin(["DELIVERED", "IN_TRANSIT"]))
    .orderBy("id")
)
