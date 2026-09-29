from pyspark.sql import functions as F

result = df_employees.filter((F.col("hire_date") >= "2023-06-13") & F.col("is_remote"))
