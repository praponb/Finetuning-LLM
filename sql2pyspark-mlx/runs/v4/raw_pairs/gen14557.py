from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("customer_stats"), F.col("customers.id") == F.col("customer_stats.customer_id"), "inner")
    .select(
        F.col("customers.id"),
        F.col("customers.signup_date"),
        F.col("customers.credit_limit"),
        F.col("customer_stats.status_list"),
        F.col("customer_stats.stddev_discount"),
    )
    .orderBy("stddev_discount")
)
