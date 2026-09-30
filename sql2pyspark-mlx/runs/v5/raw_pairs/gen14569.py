from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date")
    .unionByName(
        df_shipments
        .filter(F.col("cost") <= 39)
        .select(F.col("shipped_date").alias("start_date"))
    )
    .distinct()
)
