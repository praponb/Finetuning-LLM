from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("employee_level").orderBy(F.col("budget").desc(), "id")
w_recent = Window.partitionBy("job_title").orderBy("row_num", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("employee_level").orderBy(F.col("row_num").desc(), "id")
w5 = Window.partitionBy("job_title").orderBy("last3_count_budget")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

valid = (
    df_departments
    .filter(F.col("dept_name") != "Marketing")
    .select("*", F.round((F.col("budget") * 1.1), 2).alias("budget_scaled"))
)

latest_match = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level").alias("employee_level"), F.col("c.job_title"))
)

latest = (
    latest_match
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

metrics = (
    latest
    .withColumn("last3_min_budget", F.min("budget").over(w_recent))
    .withColumn("last3_count_budget", F.count("budget").over(w_recent))
    .withColumn("last3_sum_budget", F.sum("budget").over(w_recent))
    .withColumn("last3_max_budget", F.max("budget").over(w_recent))
    .withColumn("seq_no", F.row_number().over(w4))
)

ranked = (
    metrics
    .withColumn("row_num_2", F.rank().over(w5))
    .filter(F.col("row_num_2") == 1)
)

with_latest = (
    ranked
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.location",
        "s.budget",
        "s.budget_scaled",
        "s.employee_level",
        "s.job_title",
        "s.row_num",
        "s.last3_min_budget",
        F.col("c.salary"),
    )
)

kept = (
    with_latest
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.manager_id").isin([11, 30, 39])), "left_semi")
)

result = (
    kept
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("location") == "Singapore", F.col("budget")).otherwise(0)).alias("singapore_budget"),
        F.percentile_approx("budget_scaled", 0.9).alias("budget_scaled_p90"),
        F.count("*").alias("cnt"),
        F.round(F.avg("last3_min_budget"), 2).alias("avg_last3_min_budget"),
        F.stddev_samp("budget").alias("budget_stddev"),
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_rows"),
    )
)
