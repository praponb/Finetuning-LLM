from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .select("manager_id")
    .subtract(df_employees.filter(F.col("dept_id") == 32).select("manager_id"))
)
