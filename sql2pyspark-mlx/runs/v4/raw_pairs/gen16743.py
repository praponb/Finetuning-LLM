from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("%Car%") & F.col("dept_id").isin([19, 27, 39]))
    .select(
        "job_title",
        "level",
        "dept_id",
        F.add_months("hire_date", 1).alias("hire_date_plus_months"),
    )
    .orderBy("level")
)
