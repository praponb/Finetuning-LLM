from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location")
w3 = Window.partitionBy("dept_name", "location").orderBy("budget")
w4 = Window.partitionBy("location", "dept_name").orderBy("location_count_budget", "id")
w5 = Window.partitionBy("dept_name")
w6 = Window.partitionBy("dept_name").orderBy(F.col("prev_budget_bucket").asc_nulls_first(), "id")
w7 = Window.orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("location").orderBy(F.col("next_budget_bucket").desc_nulls_first())
w9 = Window.partitionBy("dept_name", "location").orderBy(F.col("budget_bucket").desc(), "id")
w10 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

with_windows = (
    df_departments
    .filter(~F.col("location").isin(["Berlin", "Singapore"]))
    .withColumn("budget_bucket", F.ntile(5).over(w))
    .withColumn("location_count_budget", F.count("budget").over(w2))
    .withColumn("budget_rank", F.rank().over(w3))
    .withColumn("budget_bucket_2", F.ntile(4).over(w))
)

metrics = (
    with_windows
    .select(
        "*",
        F.lag("budget_bucket").over(w4).alias("prev_budget_bucket"),
        F.lead("budget_bucket").over(w4).alias("next_budget_bucket"),
        F.lead("budget_rank").over(w4).alias("next_budget_rank"),
        F.row_number().over(w4).alias("seq_no"),
        F.sum("location_count_budget").over(w5).alias("dept_name_sum_location_count_budget"),
    )
)

latest = (
    metrics
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

eligible = (
    latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.dept_id").isin([6, 26, 38])), "left_semi")
)

scored = (
    eligible
    .withColumn("location_min_location_count_budget", F.min("location_count_budget").over(w2))
    .withColumn("running_next_budget_bucket", F.sum("next_budget_bucket").over(w7))
    .withColumn("next_budget_bucket_percent_rank", F.percent_rank().over(w8))
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w5)), 4))
)

latest_departments = (
    scored
    .withColumn("row_num", F.row_number().over(w9))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "dept_name",
        "location",
        "budget_bucket",
        "budget_rank",
        "next_budget_bucket",
        "next_budget_rank",
        "rn",
        "location_min_location_count_budget",
        "running_next_budget_bucket",
        "next_budget_bucket_percent_rank",
        "budget_share",
    )
)

with_latest = (
    latest_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w10))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.job_title"), F.col("c.salary").alias("employee_salary"))
)

result = (
    with_latest
    .rollup("job_title")
    .agg(
        F.round(F.avg("running_next_budget_bucket"), 2).alias("avg_running_next_budget_bucket"),
        F.countDistinct("job_title").alias("distinct_job_titles"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("avg_running_next_budget_bucket") < 1000000)
)
