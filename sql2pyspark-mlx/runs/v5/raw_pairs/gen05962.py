from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(F.lit("subscription").alias("origin"), "start_date")
    .union(
        df_inventory
        .filter(F.col("last_restock") <= "2025-04-29")
        .select(F.lit("inventory").alias("origin"), "last_restock")
    )
    .distinct()
)
