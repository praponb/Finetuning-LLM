from pyspark.sql import functions as F

result = df_app_logs.select("id", "level", F.explode_outer("context").alias("k", "v"))
