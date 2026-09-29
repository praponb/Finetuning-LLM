from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "location")
w2 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")

with_dims = (
    df_employees
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.location") != "Singapore")
    .select("e.*", F.col("d.location"), F.col("d.dept_name"))
)

enriched = (
    with_dims
    .select(
        "id",
        "dept_id",
        "manager_id",
        "salary",
        "location",
        "dept_name",
        F.min("salary").over(w).alias("level_min_salary"),
        F.first("job_title", ignorenulls=True).over(w2).alias("first_known_job_title"),
        F.ntile(4).over(w3).alias("salary_bucket"),
    )
)

remaining_keys = (
    enriched
    .select("manager_id", "id")
    .exceptAll(
        df_employees
        .filter(F.col("level").isNotNull())
        .select("manager_id", "id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter((~F.col("dept_name").eqNullSafe(F.lit("HR"))) & F.col("manager_id").isNotNull())
    .select("*", F.coalesce("location", F.lit("N/A")).alias("location_filled"))
)

enriched_employees = (
    cleaned
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.dept_id",
        "a.manager_id",
        "a.salary",
        "a.level_min_salary",
        "a.first_known_job_title",
        "a.location_filled",
        F.col("p.salary").alias("employee_salary"),
        F.col("p.email"),
    )
)

result = (
    enriched_employees
    .groupBy(
        F.when(F.col("level_min_salary") >= 153052, "LARGE").otherwise("SMALL").alias("level_min_salary_tier"),
    )
    .agg(
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.max("level_min_salary").alias("max_level_min_salary"),
        F.round(F.avg("employee_salary"), 2).alias("avg_employee_salary"),
    )
    .filter(F.col("max_level_min_salary") > 0)
)
