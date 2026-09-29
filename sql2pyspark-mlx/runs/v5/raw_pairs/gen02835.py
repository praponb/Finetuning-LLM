from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@company.co") & F.col("full_name").isNotNull())
    .select(
        "id",
        "email",
        "dept_id",
        "salary",
        F.add_months("hire_date", 1).alias("hire_date_plus_months"),
    )
)
