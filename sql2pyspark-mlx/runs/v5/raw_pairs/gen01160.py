from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.age") >= 33)
    .groupBy(F.col("customers.city"))
    .agg(F.sort_array(F.collect_set(F.col("sessions.browser"))).alias("browser_list"))
    .orderBy(F.col("city").desc())
)
