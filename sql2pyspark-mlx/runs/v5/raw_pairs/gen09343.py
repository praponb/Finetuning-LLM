from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.upper("email").like("%OB.%") | F.col("segment").isin(["SMB", "Consumer"]))
    .select(
        "is_active",
        "email",
        "signup_date",
        (F.col("credit_limit") * F.col("age")).alias("credit_limit_times_age"),
    )
)
