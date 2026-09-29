from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(F.col("o.status").isin(["COMPLETED", "CANCELLED", "RETURNED"]))
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(
        F.avg(F.col("o.discount")).alias("avg_discount"),
        F.max(F.col("o.order_date")).alias("latest_order_date"),
    )
    .orderBy(F.col("avg_discount").desc(), "signup_year")
    .limit(5)
)
