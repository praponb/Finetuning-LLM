from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left")
    .groupBy(
        F.date_trunc("MONTH", F.col("customers.signup_date")).alias("signup_month"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("subscriptions.status")))).alias("status_values"),
    )
    .orderBy("signup_month")
)
