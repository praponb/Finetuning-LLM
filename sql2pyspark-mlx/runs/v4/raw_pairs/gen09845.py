from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("c.city"),
        F.col("c.email"),
        F.last_day(F.col("c.signup_date")).alias("signup_date_month_end"),
    )
)
