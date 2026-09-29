from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date")
    .subtract(
        df_shipments
        .filter(F.col("carrier").isNull())
        .select(F.col("delivered_date").alias("start_date"))
    )
)
