from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(F.col("sale_date").alias("event_date"))
    .intersect(
        df_customers
        .filter(F.col("signup_date") <= "2024-08-01")
        .select(F.col("signup_date").alias("event_date"))
    )
)
