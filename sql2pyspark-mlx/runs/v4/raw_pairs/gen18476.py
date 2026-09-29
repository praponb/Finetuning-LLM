from pyspark.sql import functions as F

result = (
    df_sessions
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.signup_date"), F.col("p.name"), F.col("p.age"))
)
