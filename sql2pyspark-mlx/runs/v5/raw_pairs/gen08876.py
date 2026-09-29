from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.filter(F.col("signup_date").between("2023-11-03", "2024-05-01")).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .select(F.col("s.id"), F.col("s.end_date"))
    .orderBy("id")
    .limit(10)
)
