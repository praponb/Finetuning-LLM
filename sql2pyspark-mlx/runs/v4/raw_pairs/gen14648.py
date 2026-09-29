from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Engineer", "Intern", "Designer"]))
    .select("id", "hire_date", "level")
    .unionByName(
        df_employees
        .filter(F.col("salary").between(60000, 125000))
        .select("id", "hire_date", "level")
    )
)
