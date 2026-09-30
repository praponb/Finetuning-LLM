from pyspark.sql import functions as F

result = df_payments.select("id", "amount", "method", "paid_at")
