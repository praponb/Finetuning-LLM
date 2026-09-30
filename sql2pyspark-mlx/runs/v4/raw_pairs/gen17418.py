from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2023-04-01")
    .select("status")
    .unionByName(df_payments.filter(F.col("amount") > 1420).select("status"))
    .distinct()
)
