from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.sort_array(F.collect_set("country")).alias("country_list"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("c.email"),
        F.col("t.country_list"),
    )
    .orderBy(F.col("signup_date").desc(), F.col("email").desc())
)
