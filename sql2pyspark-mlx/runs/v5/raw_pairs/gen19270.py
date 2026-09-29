from pyspark.sql import functions as F

result = (
    df_customers
    .select("country")
    .unionByName(df_sessions.filter(F.col("customer_id").isNotNull()).select("country"))
)
