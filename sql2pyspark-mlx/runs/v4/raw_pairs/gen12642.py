from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .groupBy(
        F.col("customers.country"),
        F.date_format(F.col("customers.signup_date"), "yyyy-MM").alias("signup_month"),
    )
    .agg(F.count(F.when(F.col("orders.channel") == "web", 1)).alias("web_count"))
    .orderBy("signup_month")
)
