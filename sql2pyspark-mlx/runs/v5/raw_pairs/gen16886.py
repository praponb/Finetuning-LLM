from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.city").isin(["Singapore", "New York"]))
    .groupBy(F.quarter(F.col("c.signup_date")).alias("signup_quarter"))
    .agg(
        F.count("*").alias("num_orders"),
        F.max(F.col("o.amount")).alias("max_amount"),
    )
    .orderBy("max_amount")
)
