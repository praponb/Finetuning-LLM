from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2024-02-10").cast("date")) & (F.lower(F.trim("email")) == "liam@outlook.com"))
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.initcap(F.trim("level")).alias("level_title"),
        F.round("salary", 2).alias("salary_rounded"),
    )
)

result = (
    cleaned
    .select("id", "manager_id", "salary_rounded", "salary")
    .unpivot(["id", "manager_id"], ["salary_rounded", "salary"], "measure", "value")
    .filter(F.col("value").isNotNull())
)
