from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.year("order_date") >= 2024)
    .select("id", "store_id", "customer_id")
    .subtract(
        df_orders
        .filter(F.col("discount") <= 0.16)
        .select("id", "store_id", "customer_id")
    )
)
