from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.is_active"), F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(
        F.sort_array(F.collect_set(F.col("a.status"))).alias("status_list"),
        F.countDistinct(F.col("a.balance")).alias("unique_statuss"),
    )
    .filter(F.col("unique_statuss") >= 5)
    .orderBy(F.col("signup_year").desc())
)
