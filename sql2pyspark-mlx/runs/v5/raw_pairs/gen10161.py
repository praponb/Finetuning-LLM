from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([4, 16]) & F.col("manager_id").isNull())
    .select("id", "hire_date", "salary")
)
