from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(81000, 139000))
    .select("dept_id", "level")
    .unionByName(
        df_employees
        .filter(F.col("job_title").isNotNull())
        .select("dept_id", "level")
    )
    .distinct()
)
