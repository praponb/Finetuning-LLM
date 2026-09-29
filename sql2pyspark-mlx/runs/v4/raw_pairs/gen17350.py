from pyspark.sql import functions as F

result = (
    df_employees
    .filter(((F.col("salary") >= 85000) & F.col("full_name").like("F%")) | F.col("full_name").like("Ol%"))
    .select(
        "id",
        "email",
        "hire_date",
        "dept_id",
        F.round((F.col("salary") * 1.1), 2).alias("salary_scaled"),
    )
)
