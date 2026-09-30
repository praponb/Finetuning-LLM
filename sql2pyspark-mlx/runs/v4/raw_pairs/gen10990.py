from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["PAUSED", "ACTIVE"]))
    .select("end_date", F.lit("subscription").alias("source"))
    .unionByName(
        df_stores
        .filter(F.col("region").isin(["East", "West", "South"]))
        .select(F.col("opened_date").alias("end_date"), F.lit("store").alias("source"))
    )
    .orderBy(F.col("end_date").desc())
)
