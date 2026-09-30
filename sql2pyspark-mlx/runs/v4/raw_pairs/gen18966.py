from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") < 1410)
    .select("order_date")
    .subtract(
        df_subscriptions
        .filter(F.col("start_date") <= "2025-09-01")
        .select("end_date")
    )
)
