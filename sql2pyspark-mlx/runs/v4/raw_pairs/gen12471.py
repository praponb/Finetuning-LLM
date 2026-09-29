from pyspark.sql import functions as F

result = df_sessions.select("id", "country", F.upper("browser").alias("browser_upper"))
