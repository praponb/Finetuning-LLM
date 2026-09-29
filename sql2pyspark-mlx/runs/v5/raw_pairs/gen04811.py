from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(111, 234))
    .select(F.lit("inventory").alias("kind"), "last_restock")
    .union(
        df_subscriptions
        .select(F.lit("subscription").alias("kind"), "end_date")
    )
    .distinct()
)
