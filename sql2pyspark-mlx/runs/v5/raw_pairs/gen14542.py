from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("end_date") == 2023)
    .select(F.lit("subscription").alias("record_type"), "start_date")
    .union(
        df_orders
        .filter(F.col("discount") <= 0.36)
        .select(F.lit("order").alias("record_type"), "order_date")
    )
    .distinct()
    .orderBy("start_date")
)
