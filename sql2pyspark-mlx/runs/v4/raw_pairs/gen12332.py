from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .groupBy(
        F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"),
        F.quarter(F.col("c.signup_date")).alias("signup_quarter"),
    )
    .agg(F.sort_array(F.collect_set(F.col("o.status"))).alias("status_list"))
    .orderBy(F.col("signup_quarter").desc(), "signup_month")
    .limit(5)
)
