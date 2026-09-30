from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.filter(~F.col("segment").isin(["Consumer", "Corporate"])).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.customer_id").isin([17, 18, 31]))
    .select(F.col("s.id"), F.col("s.status"), F.col("s.plan"), F.col("s.start_date"))
    .orderBy("id")
)
