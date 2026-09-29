from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date")
    .union(
        df_customers
        .filter(F.col("signup_date") > "2025-07-04")
        .select("signup_date")
    )
    .distinct()
    .orderBy("start_date")
)
