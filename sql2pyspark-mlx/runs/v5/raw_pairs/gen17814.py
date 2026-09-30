from pyspark.sql import functions as F

result = df_payments.filter(F.col("paid_at") < "2025-01-01")
