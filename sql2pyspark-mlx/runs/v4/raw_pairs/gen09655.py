from pyspark.sql import functions as F

result = df_app_logs.select("id", "service", F.explode_outer("context").alias("k", "value"))
