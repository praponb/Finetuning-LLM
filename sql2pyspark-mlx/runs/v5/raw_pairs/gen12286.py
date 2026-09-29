from pyspark.sql import functions as F

valid = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        F.date_format("signup_date", "MMM yyyy").alias("signup_date_label"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.browser").isNotNull()), "left_anti")
)
