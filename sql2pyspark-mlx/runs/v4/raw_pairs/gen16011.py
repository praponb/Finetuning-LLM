from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("level").orderBy("salary", "id")
w_grp = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp_2 = Window.partitionBy("dept_id", "job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-04-04").cast("date")) & ~F.col("full_name").like("%Won%"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.upper("full_name").alias("full_name_upper"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("resolved_job_title"),
    )
)

scored = (
    base
    .withColumn("salary_bucket", F.ntile(5).over(w_recent))
    .withColumn("last_known_job_title", F.last("job_title", ignorenulls=True).over(w_grp))
    .withColumn("first_known_job_title", F.first("job_title", ignorenulls=True).over(w_grp_2))
)

enriched = (
    scored
    .alias("src")
    .join(df_employees.alias("p"), F.col("src.manager_id") == F.col("p.id"), "inner")
    .select(
        "src.*",
        F.col("p.full_name").alias("employee_full_name"),
        F.col("p.salary").alias("employee_salary"),
        F.col("p.job_title").alias("employee_job_title"),
    )
)

with_dims = (
    enriched
    .alias("src")
    .join(df_employees.alias("e"), F.col("src.manager_id") == F.col("e.id"), "left")
    .select(
        "src.*",
        F.col("e.job_title").alias("employee_job_title_2"),
        F.col("e.salary").alias("employee_salary_2"),
    )
)

filtered = (
    with_dims
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-05-29"))) & F.col("job_title").isNotNull())
    .select(
        "*",
        F.length("full_name").alias("full_name_length"),
        F.lower("email").alias("email_lower"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("resolved_job_title_2"),
    )
)

result = (
    filtered
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "resolved_job_title",
        F.when(F.col("level").rlike("^(L2|L1)$"), "OTHER_ZONE").when(F.col("level").rlike("^(L3|L4)$"), "GROUP_B").otherwise("PRIORITY").alias("level_zone"),
    )
)
