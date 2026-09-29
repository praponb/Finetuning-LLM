from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("plan").isin(["basic", "enterprise", "standard"]))
    .select("start_date", F.lit("subscription").alias("origin"))
    .union(
        df_inventory
        .select("last_restock", F.lit("inventory").alias("origin"))
    )
    .distinct()
    .orderBy(F.col("origin").desc())
)
