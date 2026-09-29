from pyspark.sql import functions as F

result = (
    df_employees
    .select("dept_id")
    .subtract(
        df_employees
        .filter(F.col("job_title") != "Engineer")
        .select("dept_id")
    )
)
