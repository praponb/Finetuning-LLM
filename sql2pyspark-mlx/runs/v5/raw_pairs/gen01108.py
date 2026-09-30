from pyspark.sql import functions as F

result = (
    df_payments
    .select("status", "amount")
    .orderBy("status", "amount")
)
