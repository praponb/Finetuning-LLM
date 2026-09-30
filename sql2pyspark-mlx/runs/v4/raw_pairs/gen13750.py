from pyspark.sql import functions as F

min_amount = df_payments.filter(F.col("order_id").isNull()).agg(F.min("amount")).first()[0]

result = (
    df_payments
    .filter(F.col("amount") == min_amount)
    .select("id", "amount")
)
