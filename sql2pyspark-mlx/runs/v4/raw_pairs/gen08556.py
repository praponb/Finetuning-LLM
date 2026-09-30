from pyspark.sql import functions as F

min_amount = df_payments.filter(F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"])).agg(F.min("amount")).first()[0]

result = (
    df_payments
    .filter(F.col("amount") <= min_amount)
    .select("id", "amount")
    .orderBy("id")
    .limit(3)
)
