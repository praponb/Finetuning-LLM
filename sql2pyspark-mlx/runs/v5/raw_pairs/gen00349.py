from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date")
    .union(
        df_orders
        .filter(F.col("store_id").isin([3, 12, 36, 37]))
        .select("order_date")
    )
    .distinct()
    .orderBy("signup_date")
)
