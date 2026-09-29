from pyspark.sql import functions as F

enriched = (
    df_employees
    .alias("a")
    .join(df_departments.alias("b"), F.col("a.dept_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.full_name",
        "a.email",
        "a.dept_id",
        "a.job_title",
        "a.salary",
        "a.is_remote",
        "a.level",
        F.col("b.location"),
        F.col("b.dept_name"),
    )
)

result = (
    enriched
    .select("id", "email", "dept_id", "salary", "is_remote", "level", "dept_name")
    .orderBy("salary")
)
