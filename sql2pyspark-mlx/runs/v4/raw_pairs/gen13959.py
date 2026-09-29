from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date")
    .union(
        df_accounts
        .filter(F.col("opened_date").between("2024-05-01", "2025-05-01"))
        .select("opened_date")
    )
)
