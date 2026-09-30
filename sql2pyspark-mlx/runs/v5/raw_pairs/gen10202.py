from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.country").isin(["JP", "TH"]) & (F.col("c.country") == "US"))
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.country"),
        F.col("s.started_at"),
        F.col("c.credit_limit"),
        F.col("c.country").alias("customer_country"),
    )
)
