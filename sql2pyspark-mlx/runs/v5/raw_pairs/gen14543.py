from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .union(
        df_subscriptions
        .filter(F.col("customer_id").isin([9, 16]))
        .select("start_date")
    )
    .distinct()
)
