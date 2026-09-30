from pyspark.sql import functions as F

result = (
    df_employees
    .select("manager_id", "level")
    .subtract(
        df_employees
        .filter(F.col("dept_id") == 9)
        .select("manager_id", "level")
    )
)
