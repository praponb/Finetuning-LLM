from pyspark.sql import functions as F

recent_accounts = df_accounts.filter(F.col("opened_date").between("2025-01-01", "2025-02-02"))

summary = (
    recent_accounts
    .groupBy("status")
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.count("*").alias("cnt"),
    )
)

result = (
    summary
    .orderBy("last_opened_date", "status")
    .limit(10)
)
