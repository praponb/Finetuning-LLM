from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([2, 12, 16, 35]))
    .select("status")
    .intersect(df_payments.filter(F.col("order_id").isNotNull()).select("status"))
)
