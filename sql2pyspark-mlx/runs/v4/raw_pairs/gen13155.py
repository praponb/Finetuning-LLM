from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("sale_date")
    .union(
        df_subscriptions
        .filter(F.col("customer_id").isin([12, 20, 36]))
        .select("end_date")
    )
    .distinct()
)
