from pyspark.sql import functions as F

with_dims = (
    df_sessions
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select("src.*", F.col("b.credit_limit"))
)

result = (
    with_dims
    .select("id", "customer_id", "started_at", "credit_limit")
    .orderBy("credit_limit")
)
