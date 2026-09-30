from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "FROZEN"]) & (F.col("customer_id") == 12) & F.col("customer_id").isin([2, 37]))
)
