from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")

cleaned = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-04-07").cast("date"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.coalesce("email", F.lit("Unknown")).alias("email_filled"),
        F.lower("full_name").alias("full_name_lower"),
    )
)

base = (
    cleaned
    .filter(F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "salary",
        "is_remote",
        "level",
        "email_filled",
        F.ceil("salary").alias("salary_whole"),
    )
)

base_employees = (
    base
    .filter(F.col("manager_id").isin([2, 19, 29, 36]))
    .select(
        "*",
        F.when(F.col("salary") >= 128000, "High").when(F.col("salary") >= 53000, "Medium").otherwise("Low").alias("salary_level"),
        F.coalesce("level", F.lit("N/A")).alias("resolved_level"),
    )
)

ranked = (
    base_employees
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "salary",
        "is_remote",
        "level",
        "email_filled",
        "salary_whole",
        "salary_level",
    )
)

result = (
    ranked
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id") == F.col("e.id"), "inner")
    .groupBy(
        F.col("e.level").alias("employee_level"),
        F.col("e.job_title").alias("job_title"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("a.salary_whole")).alias("total_salary_whole"),
    )
    .orderBy(F.col("job_title").desc())
)
