from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock")
    .union(
        df_shipments
        .filter(F.col("shipped_date").between("2023-03-30", "2024-02-01"))
        .select("shipped_date")
    )
    .distinct()
)
