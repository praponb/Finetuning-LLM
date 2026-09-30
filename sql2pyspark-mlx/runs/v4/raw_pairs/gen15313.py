from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("customer_id")
    .subtract(
        df_subscriptions
        .filter(F.col("start_date") > "2023-08-01")
        .select("customer_id")
    )
)
