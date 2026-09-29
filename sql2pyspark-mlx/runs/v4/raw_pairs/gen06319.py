from pyspark.sql import functions as F

result = (
    df_accounts
    .select("account_type")
    .distinct()
    .crossJoin(df_payments.select("status").distinct())
    .orderBy("account_type", "status")
)
