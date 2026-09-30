from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(129000, 130000) & (F.col("dept_id") == 24))
)
