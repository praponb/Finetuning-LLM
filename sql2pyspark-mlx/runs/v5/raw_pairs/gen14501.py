from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location")
w3 = Window.partitionBy("dept_name").orderBy("budget_share", "id")
w4 = Window.partitionBy("dept_name", "location").orderBy("row_seq", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("employee_job_title").orderBy("salary", "id").rowsBetween(-3, Window.currentRow)

enriched = (
    df_departments
    .withColumn("row_seq", F.row_number().over(w_recent))
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w2)), 4))
)

metrics = (
    enriched
    .select(
        "id",
        "row_seq",
        F.ntile(3).over(w3).alias("budget_share_bucket"),
        F.ntile(10).over(w_recent).alias("budget_bucket"),
        F.lead("budget", 2).over(w4).alias("next_budget"),
    )
)

latest_match = (
    metrics
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.budget_share_bucket",
        "s.budget_bucket",
        F.col("c.salary"),
        F.col("c.job_title").alias("employee_job_title"),
    )
)

with_windows = (
    latest_match
    .select(
        "id",
        "budget_share_bucket",
        "employee_job_title",
        F.count("budget_share_bucket").over(w_part).alias("last4_count_budget_share_bucket"),
        F.min("budget_share_bucket").over(w_part).alias("last4_min_budget_share_bucket"),
        F.sum("budget_share_bucket").over(w_part).alias("last4_sum_budget_share_bucket"),
    )
)

totals = (
    with_windows
    .groupBy(
        F.when(F.col("last4_min_budget_share_bucket") >= 9.1, "PREMIUM").otherwise("STANDARD").alias("last4_min_budget_share_bucket_tier"),
        "employee_job_title",
    )
    .agg(
        F.percentile_approx("last4_sum_budget_share_bucket", 0.95).alias("last4_sum_budget_share_bucket_p95"),
        F.size(F.collect_set("employee_job_title")).alias("num_employee_job_titles"),
        F.percentile_approx("last4_min_budget_share_bucket", 0.5).alias("last4_min_budget_share_bucket_p50"),
    )
    .filter(F.col("last4_min_budget_share_bucket_p50") >= 0)
)

result = (
    totals
    .select(
        "last4_min_budget_share_bucket_tier",
        "employee_job_title",
        "last4_sum_budget_share_bucket_p95",
    )
    .orderBy(F.col("employee_job_title").desc_nulls_last())
)
