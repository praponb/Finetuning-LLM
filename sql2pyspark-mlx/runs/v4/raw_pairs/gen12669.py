from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-06-09", "2025-02-01"))
    .select("end_date")
    .union(
        df_inventory
        .filter(F.col("store_id").isin([1, 2, 3, 5]))
        .select("last_restock")
    )
)
