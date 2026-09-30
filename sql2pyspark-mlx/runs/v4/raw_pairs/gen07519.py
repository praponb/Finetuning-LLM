from pyspark.sql import functions as F

src = (
    df_accounts
    .filter((F.year("opened_date") == 2023) & F.col("opened_date").between("2024-05-01", "2025-01-19") & (F.col("balance") <= 26000))
)

agg = (
    src
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(F.min("balance").alias("min_balance"))
)

result = agg.orderBy("opened_month")
