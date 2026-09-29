from pyspark.sql import functions as F

result = df_employees.select("full_name", F.month("hire_date").alias("hire_date_month"))
