from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.is_active"))
    .groupBy(
        F.quarter(F.col("customers.signup_date")).alias("signup_quarter"),
        F.when(F.year(F.col("customers.signup_date")) < 2024, "before_2024").when(F.year(F.col("customers.signup_date")) == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        F.avg(F.col("orders.shipping_fee")).alias("avg_shipping_fee"),
        F.min(F.col("orders.order_date")).alias("first_order_date"),
    )
)
