from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter((F.col("s.status") == "PAUSED") | ~F.col("c.is_active"))
    .select(
        F.col("s.id"),
        F.col("s.monthly_fee"),
        F.col("s.start_date"),
        F.col("s.plan"),
        F.col("c.segment"),
        F.col("c.name"),
        F.upper(F.col("c.country")).alias("country_upper"),
    )
)
