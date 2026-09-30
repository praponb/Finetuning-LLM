from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.filter(F.col("signup_date").between("2023-08-03", "2023-11-01")).alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left_semi")
    .filter(F.col("subscriptions.monthly_fee") <= 170)
    .select(F.col("subscriptions.id"), F.col("subscriptions.end_date"))
)
