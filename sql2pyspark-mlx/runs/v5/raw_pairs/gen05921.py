from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("bo%"))
    .select(
        "id",
        "manager_id",
        F.round((F.col("salary") * 1.15), 2).alias("salary_adjusted"),
    )
)
