from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNull() & (F.col("full_name").like("%For%") | F.col("email").like("%@startup.io")))
    .select(
        "dept_id",
        "manager_id",
        "hire_date",
        F.round((F.col("salary") * 100), 2).alias("adjusted_salary"),
    )
)
