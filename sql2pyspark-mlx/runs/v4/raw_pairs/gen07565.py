from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.email").like("%@gmail.com"))
    .groupBy(
        F.date_trunc("MONTH", F.col("customers.signup_date")).alias("signup_month"),
        F.col("customers.country"),
    )
    .agg(
        F.countDistinct(F.col("orders.amount")).alias("unique_stores"),
        F.count("*").alias("total_count"),
    )
    .orderBy("country", "signup_month")
    .limit(5)
)
