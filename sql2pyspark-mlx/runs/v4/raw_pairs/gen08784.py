from pyspark.sql import functions as F

result = (
    df_employees
    .select("level", "dept_id")
    .subtract(
        df_employees
        .filter(F.col("full_name").like("%smit%"))
        .select("level", "dept_id")
    )
)
