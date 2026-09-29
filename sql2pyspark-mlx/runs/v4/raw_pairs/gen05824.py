from pyspark.sql import functions as F

result = df_app_logs.select("id", "host", "level", F.explode("context").alias("k", "value"))
