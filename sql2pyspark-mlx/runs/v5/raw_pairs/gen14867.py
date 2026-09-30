from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

base = (
    df_departments
    .filter(F.col("location").isin(["Bangkok", "Chiang Mai", "Singapore"]))
    .select(
        "*",
        F.concat("location", F.lit("/"), "dept_name").alias("location_dept_name"),
        F.coalesce("dept_name", F.lit("NONE")).alias("dept_name_filled"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (~F.col("c.level").isin(["L1", "L4"])), "left_anti")
)

with_windows = (
    not_flagged
    .select(
        "*",
        F.lag("budget", 1).over(w).alias("prev_budget"),
        F.last("budget", ignorenulls=True).over(w2).alias("last_known_budget"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"), F.col("c.hire_date"))
)

with_latest = (
    with_last
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location",
        "s.budget",
        "s.dept_name_filled",
        "s.hire_date",
        F.col("c.job_title").alias("employee_job_title"),
        F.col("c.hire_date").alias("employee_hire_date"),
    )
)

totals = (
    with_latest
    .cube("location")
    .agg(
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("location") == "London", 1)), F.lit(0)), 2).alias("budget_per_london"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.count_if(F.col("employee_job_title") == "Engineer").alias("engineer_events"),
        F.percentile_approx("budget", 0.9).alias("budget_p90"),
        F.grouping("location").alias("gid"),
    )
)

result = (
    totals
    .select("budget_per_london", "budget_p90")
    .orderBy(F.col("budget_p90").asc_nulls_last())
)
