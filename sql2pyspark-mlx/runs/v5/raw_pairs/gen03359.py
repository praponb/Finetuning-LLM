from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("dept_name")
w_grp = Window.orderBy("budget_avg_6_prior", "id")
w4 = Window.partitionBy("dept_name").orderBy("budget", "id")
w_running = Window.orderBy(F.col("budget_avg_6_prior").asc_nulls_first())
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("dept_name").orderBy(F.col("budget_avg_6_prior").desc(), "id")

not_flagged = (
    df_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.dept_id").isin([8, 15, 35])), "left_anti")
)

metrics = (
    not_flagged
    .filter(F.col("location") == "Bangkok")
    .select(
        "*",
        F.avg("budget").over(w).alias("budget_avg_6_prior"),
        F.max("budget").over(w2).alias("dept_name_max_budget"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "dept_name",
        "location",
        "budget_avg_6_prior",
        "dept_name_max_budget",
        F.row_number().over(w_grp).alias("row_seq"),
        F.ntile(5).over(w4).alias("budget_bucket"),
        F.cume_dist().over(w_running).alias("budget_avg_6_prior_cume_dist"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date").alias("employee_hire_date"))
)

eligible = (
    latest_match
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.hire_date") <= F.col("s.employee_hire_date")), "left_semi")
)

cleaned = (
    eligible
    .filter(F.col("location").isNotNull())
    .select(
        "id",
        "dept_name",
        "budget_avg_6_prior",
        "dept_name_max_budget",
        "row_seq",
        "budget_avg_6_prior_cume_dist",
        "employee_hire_date",
        F.col("budget_bucket").cast("double").alias("budget_bucket_str"),
        F.add_months("employee_hire_date", 1).alias("employee_hire_date_plus_months"),
    )
)

metrics_departments = (
    cleaned
    .select(
        "id",
        "dept_name",
        "row_seq",
        "budget_avg_6_prior_cume_dist",
        "employee_hire_date",
        "employee_hire_date_plus_months",
        F.stddev_samp("dept_name_max_budget").over(w2).alias("dept_name_stddev_samp_dept_name_max_budget"),
        F.row_number().over(w7).alias("row_seq_2"),
    )
)

result = (
    metrics_departments
    .select(
        "id",
        "dept_name",
        "row_seq",
        "budget_avg_6_prior_cume_dist",
        "employee_hire_date",
        "employee_hire_date_plus_months",
        "row_seq_2",
    )
)
