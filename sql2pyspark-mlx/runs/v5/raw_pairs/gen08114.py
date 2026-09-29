from pyspark.sql import functions as F

result = (
    df_orders
    .select("status", F.lit("order").alias("kind"))
    .unionByName(
        df_subscriptions
        .filter(F.col("start_date").between("2024-07-01", "2024-11-01"))
        .select("status", F.lit("subscription").alias("kind"))
    )
    .orderBy(F.col("status").desc())
)
