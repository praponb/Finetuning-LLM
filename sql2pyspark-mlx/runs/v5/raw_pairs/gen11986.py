from pyspark.sql import functions as F

valid = (
    df_employees
    .filter(F.col("job_title") == "Engineer")
    .select(
        "*",
        F.when(F.col("job_title") == "Intern", 1).otherwise(0).alias("is_intern"),
    )
)

result = (
    valid
    .select("manager_id", "job_title")
    .subtract(
        df_employees
        .filter(F.col("hire_date") > "2023-08-08")
        .select("manager_id", "job_title")
    )
)
