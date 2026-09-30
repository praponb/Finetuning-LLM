from pyspark.sql import functions as F

result = df_employees.select("salary", "full_name", "manager_id")
