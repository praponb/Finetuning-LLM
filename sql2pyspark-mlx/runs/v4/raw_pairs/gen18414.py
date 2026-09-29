from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.year("hire_date") >= 2024)
    .select("level", "hire_date")
    .unionByName(
        df_employees
        .filter(F.col("job_title") != "Designer")
        .select("level", "hire_date")
    )
    .distinct()
    .orderBy("hire_date")
)
