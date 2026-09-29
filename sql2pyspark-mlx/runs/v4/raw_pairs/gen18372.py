from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("customers.city").isin(["Chiang Mai", "Tokyo", "Berlin"]) & (F.col("subscriptions.start_date") >= "2024-11-14"))
    .select(
        F.col("subscriptions.start_date"),
        F.col("subscriptions.customer_id"),
        F.col("customers.signup_date"),
        F.col("customers.segment"),
        F.col("customers.city"),
        F.concat_ws(",", F.col("customers.email"), F.col("customers.country")).alias("email_country_key"),
    )
)
