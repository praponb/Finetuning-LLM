from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .intersect(df_payments.select("status"))
    .orderBy("status")
)
