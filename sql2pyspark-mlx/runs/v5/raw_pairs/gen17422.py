from pyspark.sql import functions as F

result = (
    df_stores
    .select(F.lit("store").alias("record_type"), "opened_date")
    .union(
        df_subscriptions
        .filter(F.col("start_date").between("2025-04-15", "2025-06-22"))
        .select(F.lit("subscription").alias("record_type"), "end_date")
    )
    .distinct()
    .orderBy("record_type")
)
