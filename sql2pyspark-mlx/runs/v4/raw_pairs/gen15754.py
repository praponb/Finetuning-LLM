from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("account_type") == "CHECKING", 1).otherwise(0)).alias("checking_count"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.checking_count") > 22)
    .select(
        F.col("c.id"),
        F.col("c.city"),
        F.col("t.checking_count"),
        F.col("t.account_type_list"),
    )
    .orderBy(F.col("checking_count").desc())
)
