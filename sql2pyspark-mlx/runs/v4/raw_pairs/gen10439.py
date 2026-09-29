from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .filter(~((F.lower(F.trim(F.col("c.name"))) == "mia rossi") & F.month(F.col("s.start_date")).isin([1, 2, 10])))
    .select(
        F.col("s.end_date"),
        F.col("s.monthly_fee"),
        F.col("s.status"),
        F.col("c.segment"),
        F.col("c.email"),
    )
)
