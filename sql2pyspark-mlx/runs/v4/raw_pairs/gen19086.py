from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([2, 9, 33]))
    .select("id", "manager_id", "dept_id", "hire_date")
)
