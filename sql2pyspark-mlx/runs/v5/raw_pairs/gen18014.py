from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date")
    .unionByName(
        df_shipments
        .filter(F.col("order_id") == 6)
        .select(F.col("delivered_date").alias("start_date"))
    )
)
