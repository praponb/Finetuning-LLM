from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .groupBy(F.col("c.city"))
    .agg(
        F.round(F.avg(F.col("a.balance")), 2).alias("avg_balance"),
        F.sort_array(F.collect_set(F.col("a.account_type"))).alias("account_type_list"),
    )
    .orderBy(F.col("city").desc(), F.col("avg_balance").desc())
    .limit(50)
)
