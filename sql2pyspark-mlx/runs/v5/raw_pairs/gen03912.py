from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") < "2023-09-01")
    .select(F.col("signup_date").alias("event_date"))
    .unionByName(
        df_shipments
        .filter(F.col("status") == "RETURNED")
        .select(F.col("shipped_date").alias("event_date"))
    )
    .distinct()
)
