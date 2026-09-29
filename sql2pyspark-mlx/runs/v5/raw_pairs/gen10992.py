from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & F.col("account_type").isin(["CHECKING", "CREDIT"]))
)

customer_id_summary = (
    recent_accounts
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_count"),
    )
)

result = (
    customer_id_summary
    .filter(F.col("checking_count") > 6)
    .select("customer_id", "checking_count")
    .orderBy(F.col("checking_count").desc(), "customer_id")
    .limit(10)
)
