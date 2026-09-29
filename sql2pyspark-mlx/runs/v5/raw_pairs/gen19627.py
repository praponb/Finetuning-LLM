from pyspark.sql import functions as F

cleaned = (
    df_customers
    .filter(F.col("name").like("%smith%"))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.initcap(F.trim("city")).alias("city_title"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") >= F.col("s.signup_date")), "left_anti")
)
