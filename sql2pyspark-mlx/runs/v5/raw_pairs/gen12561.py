from pyspark.sql import functions as F

result = df_app_logs.select("id", "status_code", F.explode("context").alias("key", "value"))
