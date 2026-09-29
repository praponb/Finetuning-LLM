from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([3, 13, 22, 29]) | F.col("is_remote"))
    .select(
        "id",
        "dept_id",
        "hire_date",
        "job_title",
        F.date_format("hire_date", "yyyy-MM-dd").alias("hire_date_label"),
    )
)
