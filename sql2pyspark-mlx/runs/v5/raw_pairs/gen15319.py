from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("subscriptions.start_date") > "2025-02-01")
    .groupBy(
        F.date_format(F.col("customers.signup_date"), "yyyy-MM").alias("signup_month"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("subscriptions.start_date")).alias("last_start_date"),
    )
    .orderBy("signup_month")
    .limit(1)
)
