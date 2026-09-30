from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.lower(F.col("c.name")).like("%ran%"))
    .select(F.col("s.end_date"), F.col("c.signup_date"), F.col("c.city"))
    .orderBy("end_date")
)
