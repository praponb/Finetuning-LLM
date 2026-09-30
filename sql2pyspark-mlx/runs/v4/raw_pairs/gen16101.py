from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .groupBy(F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("o.channel") == "web", 1).otherwise(0))) / F.count("*")), 1).alias("pct_web"),
    )
    .orderBy(F.col("pct_web").desc(), F.col("signup_month").desc())
)
