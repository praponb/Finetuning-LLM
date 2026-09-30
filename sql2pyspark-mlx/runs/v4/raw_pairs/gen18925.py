from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("customer_id").isNotNull() & F.col("opened_date").between("2025-08-01", "2025-09-01")) | ~F.col("status").isin(["CLOSED", "FROZEN", "OPEN"]))
    .groupBy("account_type", "status")
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
    .orderBy(F.col("avg_balance").desc())
)
