from pyspark.sql import functions as F

result = (
    df_orders
    .select("status", F.lit("order").alias("kind"))
    .unionByName(df_subscriptions.select("status", F.lit("subscription").alias("kind")))
    .orderBy("status")
)
