from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("email") < 11)
    .select("dept_id")
    .subtract(df_employees.filter(F.col("job_title").isNull()).select("dept_id"))
)
