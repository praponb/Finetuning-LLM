from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("end_date")
    .subtract(
        df_orders
        .filter(F.col("order_date") < "2023-07-01")
        .select("order_date")
    )
)
