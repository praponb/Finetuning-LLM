from pyspark.sql import functions as F

result = df_employees.select("id", "dept_id", "is_remote", "level")
