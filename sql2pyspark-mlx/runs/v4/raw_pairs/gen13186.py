from pyspark.sql import functions as F

avg_amount = df_payments.filter(F.col("order_id").isin([21, 30, 35])).agg(F.avg("amount")).first()[0]

result = (
    df_payments
    .filter(F.col("amount") > avg_amount)
    .select("id", "amount", "method")
)
