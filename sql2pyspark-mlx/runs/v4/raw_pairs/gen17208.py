from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("level") != "L2") & F.col("hire_date").between("2023-11-01", "2025-08-11"))
    .select(
        "id",
        "email",
        "job_title",
        "dept_id",
        F.regexp_replace("email", " ", "-").alias("email_slug"),
        F.when(F.col("salary") >= 154000, "High").when(F.col("salary") >= 74000, "Medium").otherwise("Low").alias("salary_level"),
    )
)
