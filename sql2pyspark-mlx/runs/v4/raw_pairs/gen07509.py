from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([8, 17, 22, 30]) & F.col("opened_date").between("2023-10-01", "2024-11-10"))
    .orderBy("opened_date", "id")
    .limit(5)
)
