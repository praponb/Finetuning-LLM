from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(830, 1110))
    .select("status")
    .unionByName(df_payments.filter(F.col("status") == "REFUNDED").select("status"))
    .distinct()
)
