from pyspark.sql import functions as F

result = (
    df_accounts
    .select(F.lit("account").alias("record_type"), "status")
    .unionByName(df_payments.select(F.lit("payment").alias("record_type"), "status"))
    .distinct()
)
