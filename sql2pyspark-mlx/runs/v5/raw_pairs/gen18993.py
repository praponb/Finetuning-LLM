from pyspark.sql import functions as F

result = df_employees.agg(F.min("hire_date").alias("min_hire_date"))
