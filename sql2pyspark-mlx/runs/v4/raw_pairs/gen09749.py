from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("is_marketing", "id")
w2 = Window.partitionBy("location").orderBy("budget_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("budget_rounded", F.round("budget", 2))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("OTHER")))
)

eligible = (
    filtered
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (~F.col("ch.is_remote")), "left_anti")
)

base = (
    eligible
    .filter(F.col("dept_name") == "Sales")
    .select(
        "id",
        "location",
        "budget_rounded",
        "dept_name_filled",
        F.when(F.col("dept_name") == "Marketing", 1).otherwise(0).alias("is_marketing"),
    )
)

with_windows = (
    base
    .withColumn("is_marketing_bucket", F.ntile(4).over(w))
    .withColumn("running_budget_rounded", F.sum("budget_rounded").over(w2))
)

valid = (
    with_windows
    .filter(F.col("location").isNotNull())
    .select(
        "id",
        "location",
        "budget_rounded",
        "dept_name_filled",
        "is_marketing",
        "running_budget_rounded",
        (F.col("is_marketing_bucket") - F.col("budget_rounded")).alias("is_marketing_bucket_minus_budget_rounded"),
    )
)

with_latest = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.job_title").alias("employee_job_title"),
        F.col("c.hire_date"),
    )
)

result = (
    with_latest
    .groupBy(
        F.when(F.col("is_marketing_bucket_minus_budget_rounded") >= 84, "HIGH").otherwise("LOW").alias("is_marketing_bucket_minus_budget_rounded_tier"),
    )
    .agg(
        F.avg(F.when(F.col("employee_job_title") == "Director", F.col("budget_rounded"))).alias("director_avg_budget_rounded"),
        F.percentile_approx("is_marketing_bucket_minus_budget_rounded", 0.9).alias("is_marketing_bucket_minus_budget_rounded_p90"),
        F.percentile_approx("running_budget_rounded", 0.9).alias("running_budget_rounded_p90"),
    )
)
