from pyspark.sql import functions as F

result = df_departments.filter((F.col("dept_name") == "HR") & (F.col("budget") <= 3536000))
