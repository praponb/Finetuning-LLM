from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.filter(F.length("email") < 9).alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left_semi")
    .filter(F.col("subscriptions.end_date") > "2024-10-01")
    .select(
        F.col("subscriptions.id"),
        F.col("subscriptions.start_date"),
        F.col("subscriptions.end_date"),
    )
    .orderBy("start_date", "end_date")
)
