from pyspark.sql import functions as F

result = df_departments.filter(F.col("budget").between(1446000, 2832000))
