from pyspark.sql import functions as F

cleaned = (
    df_customers
    .filter(F.col("segment").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Chiang Mai"))))
    .select(
        "*",
        F.col("is_active").cast("int").alias("is_active_flag"),
        F.col("credit_limit").cast("int").alias("credit_limit_int"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.started_at") < F.col("s.signup_date")), "left_anti")
)
