from pyspark.sql import functions as F

result = (
    df_customers
    .select(F.col("signup_date").alias("event_date"))
    .unionByName(
        df_inventory
        .filter(F.col("store_id") == 5)
        .select(F.col("last_restock").alias("event_date"))
    )
    .distinct()
)
