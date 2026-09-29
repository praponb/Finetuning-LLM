from pyspark.sql import functions as F

result = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.country").isin(["JP", "SG"])), "left_anti")
)
