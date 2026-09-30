from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("c.city").isin(["New York", "Chiang Mai", "Singapore"]))
    .groupBy(F.quarter(F.col("s.opened_date")).alias("opened_quarter"), F.col("s.city"))
    .agg(
        F.sum(F.when(F.col("o.status") == "PENDING", F.col("o.amount")).otherwise(0)).alias("pending_amount"),
    )
)
