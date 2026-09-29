from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("cnt"),
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(subscriptions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "inner")
    .filter(F.col("sbc.cnt") >= 4)
    .select(
        F.col("c.credit_limit"),
        F.col("c.signup_date"),
        F.col("sbc.cnt"),
        F.col("sbc.plan_values"),
    )
)
