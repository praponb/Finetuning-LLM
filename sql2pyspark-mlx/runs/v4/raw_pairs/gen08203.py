from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(~F.col("dept_name").isin(["Engineering", "HR"]))
    .select("*", F.lower("dept_name").alias("dept_name_lower"))
)

result = (
    valid
    .select("dept_name", "id")
    .subtract(
        df_departments
        .filter(F.col("dept_name") == "Engineering")
        .select("dept_name", "id")
    )
)
