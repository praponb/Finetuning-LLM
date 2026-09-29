from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("segment").isin(["SMB", "Corporate"]) & F.col("segment").isNotNull())
    .select(
        "id",
        "is_active",
        F.date_trunc("YEAR", "signup_date").alias("signup_date_period"),
        (F.col("credit_limit") * F.col("age")).alias("credit_limit_times_age"),
    )
)
