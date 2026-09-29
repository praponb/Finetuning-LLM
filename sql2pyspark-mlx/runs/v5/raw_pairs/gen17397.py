from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(F.quarter(F.col("c.signup_date")).alias("signup_quarter"))
    .agg(
        F.sort_array(F.collect_set(F.col("o.channel"))).alias("channel_list"),
        F.round(F.stddev(F.col("o.amount")), 2).alias("stddev_amount"),
    )
)
