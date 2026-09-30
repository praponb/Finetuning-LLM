from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

eligible = (
    df_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.level").isin(["L4", "L1"])), "left_anti")
)

with_last = (
    eligible
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary").alias("employee_salary"), F.col("c.job_title"))
)

with_last_departments = (
    with_last
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.budget",
        "s.employee_salary",
        "s.job_title",
        F.col("c.hire_date").alias("employee_hire_date"),
    )
)

kept_keys = (
    with_last_departments
    .select("id")
    .intersectAll(
        df_departments
        .filter(F.col("budget") >= 3742000)
        .select("id")
    )
)

verified = (
    with_last_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("budget") >= 3319000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.percentile_approx("employee_salary", 0.95).alias("employee_salary_p95"),
        F.percentile_approx("employee_salary", [0.25, 0.5, 0.75]).alias("employee_salary_quartiles"),
        F.round(F.avg("employee_salary"), 2).alias("avg_employee_salary"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_rows"),
    )
    .orderBy(F.col("director_rows").desc())
)
