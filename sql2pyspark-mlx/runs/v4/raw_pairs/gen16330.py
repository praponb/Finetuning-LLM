from pyspark.sql import functions as F

filtered = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-02-28").cast("date")) & F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.trim("country").alias("clean_country"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.end_date") >= F.col("s.signup_date")), "left_anti")
)
