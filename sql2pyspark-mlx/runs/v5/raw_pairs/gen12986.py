from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("email").like("%@company.co"))
    .select(
        "id",
        "dept_id",
        "manager_id",
        F.round((F.col("salary") * 100), 2).alias("salary_scaled"),
    )
)
