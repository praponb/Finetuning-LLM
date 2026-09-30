from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.account_type").isin(["SAVINGS", "CREDIT"]))
    .groupBy(F.col("c.is_active"))
    .agg(
        F.count(F.when(F.col("a.account_type") == "LOAN", 1)).alias("loan_count"),
        F.max(F.col("a.opened_date")).alias("latest_opened_date"),
    )
    .filter(F.col("loan_count") > 3)
    .orderBy("latest_opened_date", F.col("loan_count").desc())
)
