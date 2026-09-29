from pyspark.sql import functions as F

result = (
    df_orders
    .select("customer_id", "store_id")
    .subtract(
        df_orders
        .filter(F.col("order_date") < "2025-08-01")
        .select("customer_id", "store_id")
    )
)
