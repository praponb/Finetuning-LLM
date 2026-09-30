from pyspark.sql import functions as F

max_amount = df_payments.filter(F.col("method") == "promptpay").agg(F.max("amount")).first()[0]

result = (
    df_payments
    .filter(F.col("amount") > max_amount)
    .select("id", "amount", "order_id")
    .orderBy("order_id")
)
