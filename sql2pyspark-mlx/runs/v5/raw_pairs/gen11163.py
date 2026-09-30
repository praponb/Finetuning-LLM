from pyspark.sql import functions as F

result = (
    df_employees
    .select("job_title")
    .subtract(
        df_employees
        .filter(F.col("job_title").isin(["Manager", "Analyst"]))
        .select("job_title")
    )
)
