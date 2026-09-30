from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("employee_level", "job_title").orderBy(F.col("employee_salary").desc_nulls_first(), "id")
w_grp = Window.partitionBy("employee_level", "job_title").orderBy("employee_hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("employee_salary", "id")
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.avg("salary").alias("avg_employee_salary"))

with_dims = (
    df_employees
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.email").alias("employee_email"),
        F.col("p.level").alias("employee_level"),
        F.col("p.hire_date").alias("employee_hire_date"),
    )
)

enriched = (
    with_dims
    .alias("src")
    .join(df_employees.alias("p"), F.col("src.manager_id") == F.col("p.id"), "left")
    .select(
        "src.*",
        F.col("p.job_title").alias("employee_job_title"),
        F.col("p.full_name").alias("employee_full_name"),
        F.col("p.salary").alias("employee_salary"),
    )
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

scored = (
    latest
    .withColumn("cumulative_salary", F.sum("salary").over(w_grp))
    .withColumn("next_salary", F.lead("salary", 1, 0).over(w3))
)

kept_keys = (
    scored
    .select("level")
    .exceptAll(
        df_employees
        .filter(F.col("level") == "L5")
        .select("level")
    )
)

verified = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

enriched_employees = (
    verified
    .alias("src")
    .join(df_employees.alias("p"), F.col("src.manager_id") == F.col("p.id"), "inner")
    .select(
        "src.*",
        F.col("p.full_name").alias("employee_full_name_2"),
        F.col("p.email").alias("employee_email_2"),
    )
)

enriched_employees_2 = (
    enriched_employees
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.location"), F.col("d.budget"), F.col("d.dept_name"))
)

result = (
    enriched_employees_2
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "employee_email",
        "employee_level",
        F.when(F.col("employee_level").rlike("^(L3|L2)$"), "EXTENDED").when(F.col("employee_level").rlike("^(L1|L4)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("employee_level_zone"),
        F.col("avg_employee_salary"),
    )
    .orderBy(F.col("level").desc(), F.col("avg_employee_salary").desc())
)
