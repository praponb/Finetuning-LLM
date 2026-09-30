from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date")
    .union(
        df_subscriptions
        .filter(F.col("customer_id").isin([1, 15, 16, 31]))
        .select("start_date")
    )
    .distinct()
)
