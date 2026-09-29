from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([8, 15]))
    .select("status")
    .intersect(
        df_subscriptions
        .filter(F.col("start_date") < "2023-11-01")
        .select("status")
    )
)
