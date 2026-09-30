from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.to_date(F.col("s.started_at")) < "2024-02-01")
    .groupBy(
        F.col("c.is_active"),
        F.quarter(F.col("c.signup_date")).alias("signup_quarter"),
    )
    .agg(F.countDistinct(F.col("s.customer_id")).alias("unique_customers"))
    .filter(F.col("unique_customers") > 5)
)
