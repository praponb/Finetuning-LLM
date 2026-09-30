from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock")
    .unionByName(
        df_subscriptions
        .filter(F.col("status").isin(["EXPIRED", "PAUSED"]))
        .select(F.col("end_date").alias("last_restock"))
    )
    .distinct()
    .orderBy("last_restock")
)
