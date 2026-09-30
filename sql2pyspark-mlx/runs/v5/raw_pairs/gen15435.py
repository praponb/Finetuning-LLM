from pyspark.sql import functions as F

result = df_sessions.select("customer_id", "started_at", "device")
