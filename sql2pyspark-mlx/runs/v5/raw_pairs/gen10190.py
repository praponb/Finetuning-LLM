from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock", F.lit("inventory").alias("origin"))
    .unionByName(
        df_subscriptions
        .select(
        F.col("end_date").alias("last_restock"),
        F.lit("subscription").alias("origin"),
    )
    )
    .distinct()
    .orderBy("origin")
)
