from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id") == 20)
    .select("id", "order_date")
    .unionByName(
        df_orders
        .filter(F.col("discount").between(0.23, 0.33))
        .select("id", "order_date")
    )
)
