from pyspark.sql import functions as F

kept_keys = (
    df_employees
    .select("id", "level")
    .subtract(
        df_employees
        .filter(F.col("email").isNotNull())
        .select("id", "level")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter((F.col("hire_date") >= F.lit("2023-08-22").cast("date")) & (~F.col("job_title").eqNullSafe(F.lit("Designer"))))
    .select(
        "*",
        F.trim("level").alias("clean_level"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.coalesce("job_title", F.lit("OTHER")).alias("job_title_filled"),
    )
)

joined = (
    base
    .alias("src")
    .join(df_employees.alias("p"), F.col("src.manager_id").eqNullSafe(F.col("p.id")), "inner")
    .select(
        "src.id",
        "src.full_name",
        "src.email",
        "src.dept_id",
        "src.manager_id",
        "src.job_title",
        "src.salary",
        "src.hire_date",
        "src.is_remote",
        "src.level",
        "src.clean_level",
        "src.is_remote_label",
        "src.job_title_filled",
        F.col("p.email").alias("employee_email"),
        F.col("p.salary").alias("employee_salary"),
    )
)

joined_employees = (
    joined
    .alias("e")
    .join(df_employees.alias("b"), F.col("e.manager_id") == F.col("b.id"), "left")
    .filter(F.col("b.dept_id").isin([12, 17, 18]))
    .select(
        "e.*",
        F.col("b.email").alias("employee_email_2"),
        F.col("b.level").alias("employee_level"),
    )
)

filtered = (
    joined_employees
    .filter(F.length("employee_email_2") < 9)
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        "is_remote_label",
        "employee_email",
        "employee_salary",
        "employee_email_2",
        F.upper(F.trim("employee_email")).alias("employee_email_normalized"),
    )
)

valid = (
    filtered
    .filter(F.col("job_title").isNotNull() & (F.col("level").eqNullSafe(F.lit("L3"))))
    .withColumn("full_name_prefix", F.substring("full_name", 1, 2))
)

with_dims = (
    valid
    .alias("a")
    .join(df_departments.alias("b"), F.col("a.dept_id") == F.col("b.id"), "inner")
    .select(
        "a.id",
        "a.full_name",
        "a.email",
        "a.dept_id",
        "a.is_remote_label",
        "a.employee_email",
        "a.employee_salary",
        "a.full_name_prefix",
        F.col("b.location"),
        F.col("b.dept_name"),
        F.col("b.budget"),
    )
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("employee_salary") >= 131000, "HIGH").otherwise("LOW").alias("employee_salary_tier"),
        "dept_id",
    )
    .agg(
        F.count_if(F.col("dept_name") == "Legal").alias("legal_events"),
        F.countDistinct("dept_id").alias("distinct_dept_ids"),
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_count"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
    )
    .orderBy(F.col("dept_id").desc_nulls_first())
)
