from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("budget") < 1512000))
    .withColumn("budget_level", F.when(F.col("budget") >= 3286000, "High").when(F.col("budget") >= 1452000, "Medium").otherwise("Low"))
)

with_last = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.job_title").alias("employee_job_title"),
        F.col("c.hire_date"),
    )
)

kept = (
    with_last
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.hire_date") >= F.col("s.hire_date")), "left_anti")
)

kept_departments = (
    kept
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.lower(F.trim(F.col("ch.email"))) == "henry@startup.io"), "left_anti")
)

remaining_keys = (
    kept_departments
    .select("dept_name", "location")
    .subtract(
        df_departments
        .filter(F.col("dept_name") == "Sales")
        .select("dept_name", "location")
    )
)

kept_rows = (
    kept_departments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

summary = (
    kept_rows
    .groupBy("location")
    .agg(
        F.stddev_samp("budget").alias("budget_stddev"),
        F.countDistinct("location").alias("distinct_locations"),
        F.count("*").alias("event_count"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
    )
    .filter(F.col("event_count") > 1)
)

result = (
    summary
    .select("location", "budget_stddev", "distinct_locations")
    .orderBy(F.col("location").asc_nulls_first(), F.col("budget_stddev").desc())
)
