from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("c.city") == "Tokyo")
    .groupBy(
        F.when(F.year(F.col("c.signup_date")) < 2024, "before_2024").when(F.year(F.col("c.signup_date")) == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        F.sort_array(F.collect_set(F.col("s.carrier"))).alias("carrier_list"),
        (F.max(F.col("s.cost")) - F.min(F.col("s.cost"))).alias("cost_range"),
    )
    .orderBy("signup_cohort")
    .limit(5)
)
