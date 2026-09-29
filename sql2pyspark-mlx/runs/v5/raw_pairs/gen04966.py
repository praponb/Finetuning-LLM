from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([9, 16, 39]))
    .select("job_title")
    .unionByName(df_employees.filter(~F.col("is_remote")).select("job_title"))
    .distinct()
)
