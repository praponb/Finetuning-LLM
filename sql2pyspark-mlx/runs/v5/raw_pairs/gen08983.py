from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") > 74000)
    .select("status")
    .intersect(df_payments.select("status"))
)
