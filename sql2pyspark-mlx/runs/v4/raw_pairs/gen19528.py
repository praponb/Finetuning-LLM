from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("c.credit_limit").isNotNull())
    .groupBy(F.quarter(F.col("s.opened_date")).alias("opened_quarter"), F.col("s.city"))
    .agg(
        F.sum(F.when(F.col("o.status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
    )
    .orderBy("cancelled_count", "opened_quarter", "city")
    .limit(50)
)
