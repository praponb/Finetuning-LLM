from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .union(
        df_subscriptions
        .filter(F.col("customer_id").isin([10, 35]))
        .select("end_date")
    )
)
