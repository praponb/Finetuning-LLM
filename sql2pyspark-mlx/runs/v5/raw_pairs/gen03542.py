from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date")
    .union(
        df_subscriptions
        .filter(F.col("start_date") <= "2024-05-11")
        .select("end_date")
    )
    .orderBy(F.col("order_date").desc())
)
