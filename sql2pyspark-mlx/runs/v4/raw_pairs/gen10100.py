from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date")
    .subtract(
        df_customers
        .filter(~F.col("is_active"))
        .select(F.col("signup_date").alias("start_date"))
    )
)
