from pyspark.sql import functions as F

result = df_payments.agg(F.max("paid_at").alias("latest_paid_at"))
