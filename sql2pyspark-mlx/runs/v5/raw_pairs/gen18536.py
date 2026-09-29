from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["mobile", "web"]))
    .select(F.lit("order").alias("origin"), "status")
    .unionByName(
        df_subscriptions
        .filter(F.year("start_date") >= 2024)
        .select(F.lit("subscription").alias("origin"), "status")
    )
    .distinct()
)
