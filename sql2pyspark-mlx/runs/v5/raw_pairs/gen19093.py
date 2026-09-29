from pyspark.sql import functions as F

result = df_departments.withColumn("location_title", F.initcap(F.trim("location")))
