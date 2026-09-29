from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("salary_plus_salary_rounded").desc_nulls_first())

base = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-08-26").cast("date")) & F.col("job_title").isNotNull())
    .withColumn("salary_rounded", F.round("salary", 1))
    .withColumn("email_user", F.split(F.lower("email"), "@").getItem(0))
)

remaining_keys = (
    base
    .select("dept_id")
    .exceptAll(
        df_employees
        .filter(F.col("level").isin(["L5", "L4"]))
        .select("dept_id")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

base_employees = (
    verified
    .filter((F.col("hire_date") >= F.lit("2023-04-30").cast("date")) & (F.col("level").eqNullSafe(F.lit("L3"))))
    .withColumn("salary_plus_salary_rounded", (F.col("salary") + F.col("salary_rounded")))
    .withColumn("resolved_level", F.coalesce("level", F.lit("N/A")))
)

scored = (
    base_employees
    .withColumn("salary_plus_salary_rounded_cume_dist", F.cume_dist().over(w))
)

joined = (
    scored
    .alias("s")
    .join(df_employees.alias("b"), F.col("s.manager_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.full_name",
        "s.manager_id",
        "s.job_title",
        "s.salary",
        "s.is_remote",
        "s.level",
        "s.salary_rounded",
        "s.email_user",
        "s.salary_plus_salary_rounded",
        "s.salary_plus_salary_rounded_cume_dist",
        F.col("b.full_name").alias("employee_full_name"),
        F.col("b.salary").alias("employee_salary"),
    )
)

enriched = (
    joined
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.full_name").alias("employee_full_name_2"))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_employees.alias("p2"), F.col("e.manager_id").eqNullSafe(F.col("p2.id")), "inner")
    .select("e.*", F.col("p2.level").alias("employee_level"))
)

result = (
    with_dims
    .select(
        "full_name",
        "job_title",
        "salary",
        "level",
        "email_user",
        "salary_plus_salary_rounded",
        "salary_plus_salary_rounded_cume_dist",
        "employee_salary",
        "employee_full_name_2",
        F.when(F.col("job_title").rlike("^(Manager|Director)$"), "GROUP_B").when(F.col("job_title").rlike("^(Engineer|Analyst)$"), "GROUP_A").otherwise("PRIORITY").alias("job_title_zone"),
    )
)
