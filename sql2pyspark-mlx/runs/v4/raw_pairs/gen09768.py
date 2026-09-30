from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") >= "2025-07-04")
    .select("status", "customer_id")
    .subtract(
        df_orders
        .filter(F.col("channel").isin(["mobile", "store"]))
        .select("status", "customer_id")
    )
)
