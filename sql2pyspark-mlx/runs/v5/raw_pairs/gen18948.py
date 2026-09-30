from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "job_title").orderBy(F.col("salary").desc(), "id")

valid = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-05-10").cast("date"))
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
        F.last_day("hire_date").alias("hire_date_month_end"),
        F.coalesce("level", F.lit("N/A")).alias("level_clean"),
    )
)

valid_employees = (
    valid
    .filter((F.col("hire_date") > F.lit("2023-07-07").cast("date")) & F.col("level").isNotNull())
    .select(
        "*",
        F.when(F.col("level").isin(["L3", "L2"]), "Priority").otherwise("Other").alias("level_group"),
    )
)

with_dims = (
    valid_employees
    .alias("src")
    .join(df_employees.alias("b"), F.col("src.manager_id") == F.col("b.id"), "inner")
    .filter(F.col("b.is_remote"))
    .select(
        "src.id",
        "src.full_name",
        "src.email",
        "src.manager_id",
        "src.job_title",
        "src.salary",
        "src.hire_date_month_end",
        "src.level_clean",
        F.col("b.email").alias("employee_email"),
    )
)

joined = (
    with_dims
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id") == F.col("p.id"), "left")
    .select(
        "a.*",
        F.col("p.full_name").alias("employee_full_name"),
        F.col("p.email").alias("employee_email_2"),
        F.col("p.hire_date"),
    )
)

ranked = (
    joined
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    ranked
    .alias("s")
    .join(df_employees.alias("p"), F.col("s.manager_id").eqNullSafe(F.col("p.id")), "inner")
    .select("s.*", F.col("p.salary").alias("employee_salary"), F.col("p.level"))
)

joined_employees = (
    enriched
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id").eqNullSafe(F.col("e.id")), "left")
    .select(
        "a.id",
        "a.full_name",
        "a.email",
        "a.manager_id",
        "a.salary",
        "a.level_clean",
        "a.employee_email",
        "a.employee_full_name",
        "a.employee_email_2",
        "a.rn",
        "a.employee_salary",
        "a.level",
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.full_name").alias("employee_full_name_2"),
    )
)

totals = (
    joined_employees
    .groupBy("employee_job_title")
    .agg(
        F.sum(F.when(F.col("employee_job_title") == "Designer", F.col("employee_salary"))).alias("designer_total_employee_salary"),
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_rows"),
    )
)

result = (
    totals
    .select(
        "employee_job_title",
        "designer_total_employee_salary",
        "l2_rows",
        F.when(F.col("employee_job_title").rlike("^(Analyst|Designer)$"), "GROUP_A").when(F.col("employee_job_title").rlike("^(Engineer|Manager)$"), "GROUP_B").otherwise("PRIORITY").alias("employee_job_title_zone"),
    )
    .orderBy("employee_job_title_zone", F.col("l2_rows").desc())
)
