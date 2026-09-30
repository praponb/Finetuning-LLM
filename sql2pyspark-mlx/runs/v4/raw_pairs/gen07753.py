from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("customer_id").isin([3, 14, 22]) & F.col("customer_id").isNull()) | (F.col("customer_id") == 27))
)
