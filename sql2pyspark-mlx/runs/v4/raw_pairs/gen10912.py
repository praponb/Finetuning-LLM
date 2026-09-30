from pyspark.sql import functions as F

max_amount = df_payments.filter(F.col("amount").between(1010, 1110)).agg(F.max("amount")).first()[0]

result = (
    df_payments
    .filter(F.col("amount") > max_amount)
    .select("id", "amount")
    .orderBy(F.col("id").desc(), F.col("amount").desc())
    .limit(5)
)
