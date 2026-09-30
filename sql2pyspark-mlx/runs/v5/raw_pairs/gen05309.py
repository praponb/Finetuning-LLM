from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .select("dept_id")
    .subtract(df_employees.filter(F.col("email").isNotNull()).select("dept_id"))
    .orderBy("dept_id")
)
