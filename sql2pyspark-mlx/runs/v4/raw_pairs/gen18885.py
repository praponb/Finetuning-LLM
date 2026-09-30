from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())
w2 = Window.orderBy("employee_hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("job_title", "dept_id").orderBy("employee_hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_employees
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_dims = (
    latest
    .alias("a")
    .join(df_employees.alias("b"), F.col("a.manager_id") == F.col("b.id"), "inner")
    .select(
        "a.*",
        F.col("b.hire_date").alias("employee_hire_date"),
        F.col("b.level").alias("employee_level"),
        F.col("b.email").alias("employee_email"),
    )
)

with_windows = (
    with_dims
    .select(
        "*",
        F.first("job_title", ignorenulls=True).over(w2).alias("first_known_job_title"),
        F.sum("salary").over(w3).alias("running_salary"),
    )
)

joined = (
    with_windows
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.dept_name"))
)

enriched = (
    joined
    .alias("a")
    .join(df_employees.alias("d"), F.col("a.manager_id") == F.col("d.id"), "inner")
    .select(
        "a.*",
        F.col("d.job_title").alias("employee_job_title"),
        F.col("d.level").alias("employee_level_2"),
    )
)

base = (
    enriched
    .filter(F.col("manager_id").isNotNull())
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
        "employee_hire_date",
        "employee_level",
        "employee_email",
        "first_known_job_title",
        "running_salary",
        "dept_name",
        "employee_job_title",
        "employee_level_2",
        F.date_add("hire_date", 46).alias("hire_date_plus_46d"),
    )
)

by_type = (
    base
    .groupBy("job_title")
    .pivot("employee_level_2", ["L1", "L5", "L4"])
    .agg(F.sum("running_salary"))
)

result = (
    by_type
    .select(
        "job_title",
        "L5",
        "L4",
        F.when(F.col("job_title").rlike("^(Designer|Analyst)$"), "GROUP_A").when(F.col("job_title").rlike("^(Engineer|Manager)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("job_title_zone"),
    )
    .orderBy("L5")
)
