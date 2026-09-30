from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_last())
w_hist = Window.partitionBy("location")
w_hist_2 = Window.partitionBy("location").orderBy("budget", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("employee_level").orderBy("employee_salary_2", "id")
w6 = Window.partitionBy("employee_job_title").orderBy("employee_salary_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_departments
    .filter(F.col("dept_name") == "Engineering")
    .withColumn("budget_rank", F.rank().over(w))
    .withColumn("location_stddev_samp_budget", F.stddev_samp("budget").over(w_hist))
    .withColumn("prev_budget", F.lag("budget", 2).over(w_hist_2))
    .withColumn("location_max_budget", F.max("budget").over(w_hist))
)

with_last = (
    enriched
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location",
        "s.budget",
        "s.location_stddev_samp_budget",
        "s.prev_budget",
        F.col("c.level").alias("employee_level"),
        F.col("c.salary").alias("employee_salary"),
    )
)

with_latest = (
    with_last
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.budget",
        "s.prev_budget",
        "s.employee_salary",
        F.col("c.job_title").alias("employee_job_title"),
        F.col("c.level"),
    )
)

with_last_departments = (
    with_latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.prev_budget",
        "s.employee_job_title",
        "s.level",
        F.col("c.level").alias("employee_level"),
        F.col("c.salary").alias("employee_salary_2"),
    )
)

not_flagged = (
    with_last_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.full_name").like("M%")), "left_anti")
)

metrics = (
    not_flagged
    .withColumn("next_employee_salary_2", F.lead("employee_salary_2").over(w_part))
    .withColumn("prev_employee_salary_2", F.lag("employee_salary_2").over(w_part))
    .withColumn("next_prev_budget", F.lead("prev_budget").over(w_part))
    .withColumn("event_seq", F.row_number().over(w_part))
    .withColumn("running_prev_budget", F.sum("prev_budget").over(w6))
)

result = (
    metrics
    .select(
        "id",
        "prev_budget",
        "employee_job_title",
        "employee_level",
        "employee_salary_2",
        "prev_employee_salary_2",
        "event_seq",
        F.when(F.col("employee_level").rlike("^(L4|L3)$"), "PRIORITY").when(F.col("employee_level").rlike("^(L1|L2)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("employee_level_zone"),
    )
)
