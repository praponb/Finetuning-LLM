from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.month("hire_date").isin([4, 5, 6]) & (F.length("email") < 11))
    .select(
        "full_name",
        "level",
        "dept_id",
        F.round("salary", 2).alias("salary_rounded"),
    )
)
