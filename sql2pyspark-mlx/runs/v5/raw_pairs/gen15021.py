from pyspark.sql import functions as F

kept_keys = (
    df_employees
    .select("manager_id", "dept_id")
    .exceptAll(
        df_employees
        .filter(F.col("job_title") != "Director")
        .select("manager_id", "dept_id")
    )
)

reconciled = (
    df_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.when(F.col("job_title").rlike("^(Intern|Engineer)$"), "EXTENDED").when(F.col("job_title").rlike("^(Analyst|Manager)$"), "GROUP_B").otherwise("GROUP_A").alias("job_title_zone"),
    )
    .orderBy("id", "job_title_zone")
)
