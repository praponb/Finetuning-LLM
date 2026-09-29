from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(F.col("end_date").alias("event_date"))
    .unionByName(
        df_stores
        .filter(F.month("opened_date").isin([1, 3, 4]))
        .select(F.col("opened_date").alias("event_date"))
    )
    .orderBy("event_date")
)
