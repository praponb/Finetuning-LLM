from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "partner")
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.col("start_date").between("2024-05-01", "2025-09-01"))
        .select("status")
    )
    .distinct()
    .orderBy("status")
)
