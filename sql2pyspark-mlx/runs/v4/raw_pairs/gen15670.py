from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("%Brown%"))
    .select("id", "manager_id")
    .subtract(
        df_employees
        .filter(F.month("hire_date").isin([2, 5, 6]))
        .select("id", "manager_id")
    )
)
