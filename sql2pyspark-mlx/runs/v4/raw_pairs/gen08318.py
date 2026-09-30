from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([1, 6, 11]))
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "status")
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.max("opened_date").alias("last_opened_date"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
    )
    .orderBy(F.col("opened_quarter").desc())
)
