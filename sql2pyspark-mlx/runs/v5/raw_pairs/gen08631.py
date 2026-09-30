from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("dept_name").orderBy("employee_hire_date", "id")
w_grp = Window.partitionBy("dept_name").orderBy("budget")
w4 = Window.partitionBy("dept_name").orderBy(F.col("event_seq").asc_nulls_last(), "id")
w5 = Window.partitionBy("location").orderBy("prev_budget")

valid = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "Bangkok", "Chiang Mai"]))
    .select("*", F.coalesce("location", F.lit("Unknown")).alias("location_filled"))
)

with_last = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.location",
        "s.budget",
        F.col("c.job_title").alias("employee_job_title"),
        F.col("c.hire_date").alias("employee_hire_date"),
    )
)

with_windows = (
    with_last
    .select(
        "*",
        F.lag("budget").over(w_running).alias("prev_budget"),
        F.row_number().over(w_running).alias("event_seq"),
        F.dense_rank().over(w_grp).alias("budget_dense_rank"),
    )
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "location",
        "employee_hire_date",
        "prev_budget",
        "event_seq",
        "budget_dense_rank",
    )
)

eligible = (
    latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") < F.col("s.employee_hire_date")), "left_semi")
)

latest_departments = (
    eligible
    .withColumn("row_num", F.rank().over(w5))
    .filter(F.col("row_num") == 1)
)

grouped = (
    latest_departments
    .withColumn("prev_budget_tier", F.when(F.col("prev_budget") >= 4236000, "HIGH").otherwise("LOW"))
    .cube("prev_budget_tier")
    .agg(
        F.size(F.collect_set("location")).alias("num_locations"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.grouping("prev_budget_tier").alias("gid"),
    )
)

result = grouped.select("num_locations", "event_seq_quartiles")
