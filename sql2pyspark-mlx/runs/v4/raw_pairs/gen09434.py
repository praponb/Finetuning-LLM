from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("budget_rounded", F.round("budget", 0))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("OTHER")))
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.job_title").alias("employee_job_title"),
        F.col("c.hire_date").alias("employee_hire_date"),
    )
)

remaining_keys = (
    with_latest
    .select("id", "location")
    .subtract(
        df_departments
        .filter(F.col("budget") > 2120000)
        .select("id", "location")
    )
)

kept_rows = (
    with_latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.hire_date") <= F.col("s.employee_hire_date")), "left_anti")
)

result = (
    kept
    .groupBy(F.date_trunc("QUARTER", "employee_hire_date").alias("quarter_start"))
    .agg(
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("employee_job_title") == "Analyst", 1)), F.lit(0)), 2).alias("budget_per_analyst"),
        F.count(F.when(F.col("employee_job_title") == "Engineer", 1)).alias("engineer_count"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("engineer_count") >= 1)
    .orderBy(F.col("quarter_start").desc())
)
