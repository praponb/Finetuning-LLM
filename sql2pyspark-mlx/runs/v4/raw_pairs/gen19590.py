from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") | F.col("is_remote"))
    .select(
        "dept_id",
        "manager_id",
        "is_remote",
        F.date_add("hire_date", 51).alias("hire_date_plus_51d"),
    )
)
