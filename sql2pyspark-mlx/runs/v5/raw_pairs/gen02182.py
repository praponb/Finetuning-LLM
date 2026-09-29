from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .filter(F.col("a.status").isin(["OPEN", "CLOSED"]))
    .groupBy(
        F.year(F.col("c.signup_date")).alias("signup_year"),
        F.when(F.col("c.city").isin(["New York", "Berlin"]), "core").otherwise("other").alias("city_group"),
    )
    .agg(F.sort_array(F.collect_set(F.col("a.status"))).alias("status_list"))
)
