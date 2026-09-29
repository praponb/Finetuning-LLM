from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("id", "customer_id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("start_date").between("2024-01-11", "2025-06-04"))
        .select("id", "customer_id")
    )
)
