from pyspark.sql import functions as F

result = (
    df_sessions
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.signup_date"), F.col("d.segment"), F.col("d.name"))
)
