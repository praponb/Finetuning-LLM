from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .unionByName(df_payments.filter(F.col("status") != "SUCCESS").select("status"))
    .distinct()
)
