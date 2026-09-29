from pyspark.sql import functions as F

valid = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "email",
        "country",
        "segment",
        "is_active",
        (F.col("credit_limit") - F.col("age")).alias("credit_limit_minus_age"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.device") == "desktop"), "left_anti")
)
