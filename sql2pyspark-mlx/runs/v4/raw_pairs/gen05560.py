from pyspark.sql import functions as F

result = (
    df_shipments
    .select("shipped_date")
    .subtract(df_customers.select(F.col("signup_date").alias("shipped_date")))
)
