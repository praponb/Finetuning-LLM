from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.upper(F.trim("email")) == "ALICE@GMAIL.COM") & ~F.col("city").isin(["Bangkok", "Tokyo"]))
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.max("signup_date").alias("latest_signup_date"),
        F.max("signup_date").alias("last_signup_date"),
        F.min("credit_limit").alias("min_credit_limit"),
    )
    .orderBy(F.col("signup_month").desc())
)
