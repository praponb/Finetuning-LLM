from pyspark.sql import functions as F

result = (
    df_orders
    .select("channel")
    .exceptAll(
        df_orders
        .filter(F.col("order_date").between("2024-09-01", "2025-09-11"))
        .select("channel")
    )
)
