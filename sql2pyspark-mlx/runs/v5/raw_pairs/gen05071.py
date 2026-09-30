from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("hire_date").asc_nulls_first(), "id")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.max("salary").alias("max_employee_salary"))

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-05-21"))) & (F.col("job_title").eqNullSafe(F.lit("Designer"))))
    .select(
        "*",
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
        F.coalesce("level", F.lit("DIRECT")).alias("resolved_level"),
    )
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

cleaned_employees = (
    deduped
    .filter(~F.col("job_title").eqNullSafe(F.lit("Designer")))
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "level",
        "email_user",
        "resolved_level",
        "rank_in_group",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        (F.col("rank_in_group") + F.col("salary")).alias("rank_in_group_plus_salary"),
        F.coalesce("level", F.lit("DIRECT")).alias("level_filled"),
    )
)

joined = (
    cleaned_employees
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id") == F.col("p.id"), "left")
    .select(
        "a.*",
        F.col("p.level").alias("employee_level"),
        F.col("p.hire_date").alias("employee_hire_date"),
    )
)

result = (
    joined
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "dept_id",
        "level",
        "email_user",
        "resolved_level",
        "rank_in_group",
        "is_remote_flag",
        "rank_in_group_plus_salary",
        F.col("max_employee_salary"),
    )
)
