from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("location", "job_title").orderBy("budget", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("dept_name")
w4 = Window.orderBy(F.col("budget_avg_6").asc_nulls_first())
w5 = Window.partitionBy("job_title").orderBy(F.col("budget").desc(), "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

latest_match = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.hire_date").alias("employee_hire_date"),
        F.col("c.job_title"),
    )
)

with_windows = (
    latest_match
    .select(
        "*",
        F.avg("budget").over(w2).alias("budget_avg_6"),
        F.round((F.col("budget") / F.sum("budget").over(w3)), 4).alias("budget_share"),
    )
)

with_windows_departments = (
    with_windows
    .filter(F.col("budget_share") >= 0.62)
    .select(
        "id",
        "dept_name",
        "location",
        "budget_share",
        F.avg("budget_avg_6").over(w3).alias("dept_name_avg_budget_avg_6"),
        F.dense_rank().over(w4).alias("budget_avg_6_dense_rank"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

with_last = (
    with_windows_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name_avg_budget_avg_6",
        "s.budget_avg_6_dense_rank",
        F.col("c.job_title").alias("employee_job_title"),
    )
)

eligible = (
    with_last
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.dept_id").isin([1, 18])), "left_semi")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date"))
)

result = (
    with_latest
    .groupBy("employee_job_title")
    .agg(
        F.size(F.collect_set("employee_job_title")).alias("num_employee_job_titles"),
        F.count(F.when(F.col("employee_job_title") == "Designer", 1)).alias("designer_rows"),
        F.percentile_approx("budget_avg_6_dense_rank", [0.25, 0.5, 0.75]).alias("budget_avg_6_dense_rank_quartiles"),
    )
)
