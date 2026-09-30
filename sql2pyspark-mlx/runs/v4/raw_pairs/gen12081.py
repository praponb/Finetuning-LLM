from pyspark.sql import functions as F

result = (
    df_shipments
    .select("shipped_date")
    .subtract(
        df_subscriptions
        .filter(F.col("end_date") >= "2024-02-01")
        .select(F.col("start_date").alias("shipped_date"))
    )
)
