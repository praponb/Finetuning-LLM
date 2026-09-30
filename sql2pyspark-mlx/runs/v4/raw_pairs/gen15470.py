from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Manager", "Director", "Engineer"]))
    .select("hire_date", "level")
    .subtract(
        df_employees
        .filter(F.col("full_name").like("%Chai%"))
        .select("hire_date", "level")
    )
)
