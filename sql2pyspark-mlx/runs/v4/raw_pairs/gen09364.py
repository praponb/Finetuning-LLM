from pyspark.sql import functions as F

result = df_payments.filter(F.col("order_id").isNull() | (F.col("status") == "FAILED"))
