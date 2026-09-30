from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("total_count"),
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance")).otherwise(0)).alias("checking_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.checking_balance") >= 34)
    .select(
        F.col("c.city"),
        F.col("c.age"),
        F.col("cs.total_count"),
        F.col("cs.checking_balance"),
    )
    .orderBy("city")
)
