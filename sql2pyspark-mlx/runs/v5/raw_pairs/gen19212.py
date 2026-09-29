from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.city") != "Bangkok")
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"), F.col("c.is_active"))
    .agg(
        F.min(F.col("a.balance")).alias("min_balance"),
        F.sort_array(F.collect_set(F.col("a.status"))).alias("status_list"),
    )
    .orderBy("signup_year", "min_balance", "is_active")
    .limit(5)
)
