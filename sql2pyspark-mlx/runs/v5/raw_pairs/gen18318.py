from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.filter(F.col("segment") == "Corporate").alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.customer_id") == 18)
    .select(F.col("s.id"), F.col("s.start_date"), F.col("s.status"), F.col("s.plan"))
)
