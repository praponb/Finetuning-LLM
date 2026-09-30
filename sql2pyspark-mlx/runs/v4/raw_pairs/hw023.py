from pyspark.sql import functions as F

result = df_employees.groupBy("dept_id").agg(F.sum("salary").alias("total_payroll"))
