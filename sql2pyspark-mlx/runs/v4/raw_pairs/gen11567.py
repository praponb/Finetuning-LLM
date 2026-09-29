from pyspark.sql import functions as F

min_amount = df_payments.filter(F.hour("paid_at").between(6, 11)).agg(F.min("amount")).first()[0]

result = (
    df_payments
    .filter(F.col("amount") == min_amount)
    .select("id", "amount")
)
