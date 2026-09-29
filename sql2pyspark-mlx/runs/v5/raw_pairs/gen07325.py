from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.filter(F.col("end_date").between("2023-09-01", "2025-06-01")).alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .filter(F.col("c.country").isin(["TH", "US", "JP"]))
    .select(F.col("c.id"), F.col("c.name"))
)
