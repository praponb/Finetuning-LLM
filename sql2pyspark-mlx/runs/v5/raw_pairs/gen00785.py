from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(
        F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"),
        F.quarter(F.col("c.signup_date")).alias("signup_quarter"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.stddev(F.col("o.discount")), 2).alias("stddev_discount"),
    )
    .filter(F.col("row_count") >= 2)
    .orderBy(F.col("signup_quarter").desc(), "signup_month")
    .limit(5)
)
