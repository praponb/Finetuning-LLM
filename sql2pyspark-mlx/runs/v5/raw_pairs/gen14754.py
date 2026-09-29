from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 26)
    .select("id", "customer_id")
    .unionByName(
        df_orders
        .filter(F.col("order_date") >= "2024-06-01")
        .select("id", "customer_id")
    )
)
