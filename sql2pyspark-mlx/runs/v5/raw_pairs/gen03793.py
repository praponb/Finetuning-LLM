from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("%smi%"))
    .select(
        "id",
        "dept_id",
        "level",
        "hire_date",
        F.lower("level").alias("level_lower"),
        F.col("salary").cast("int").alias("salary_int"),
    )
)
