from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("job_title").orderBy(F.col("salary").desc())
w3 = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("job_title").orderBy("salary_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("level").orderBy(F.col("salary").desc())
w6 = Window.partitionBy("location").orderBy(F.col("running_salary").desc(), "id")
w7 = Window.partitionBy("dept_name", "job_title").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("salary_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("dept_id").orderBy("salary_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("job_title").orderBy("running_budget", "id")
w11 = Window.partitionBy("manager_id").orderBy("budget", "id")
w12 = Window.partitionBy("job_title").orderBy("cumulative_running_salary", "id")
w13 = Window.partitionBy("manager_id", "level").orderBy("running_budget", "id").rowsBetween(-2, Window.currentRow)
w14 = Window.partitionBy("level", "manager_id")
w15 = Window.partitionBy("level", "location")
w16 = Window.partitionBy("location").orderBy(F.col("salary_bucket").asc_nulls_first())
w17 = Window.orderBy(F.col("next_salary_rank").desc(), "id")
w18 = Window.partitionBy("dept_name", "level").orderBy("first_known_budget", "id")
w19 = Window.partitionBy("manager_id", "location").orderBy("cumulative_running_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w20 = Window.partitionBy("running_budget_bucket_tier").orderBy(F.col("finance_count").desc())

with_windows = (
    df_employees
    .withColumn("salary_bucket", F.ntile(3).over(w))
    .withColumn("salary_cume_dist", F.cume_dist().over(w2))
    .withColumn("running_salary", F.sum("salary").over(w3))
)

with_dims = (
    with_windows
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.location"), F.col("d.dept_name"), F.col("d.budget"))
)

metrics = (
    with_dims
    .select(
        "*",
        F.first("budget", ignorenulls=True).over(w_ordered).alias("first_known_budget"),
        F.rank().over(w5).alias("salary_rank"),
        F.ntile(4).over(w6).alias("running_salary_bucket"),
        F.sum("running_salary").over(w7).alias("cumulative_running_salary"),
        F.sum("budget").over(w8).alias("running_budget"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.last("first_known_budget", ignorenulls=True).over(w_running).alias("last_known_first_known_budget"),
        F.ntile(3).over(w10).alias("running_budget_bucket"),
        F.lead("salary_rank", 2).over(w11).alias("next_salary_rank"),
    )
)

metrics_employees = (
    enriched
    .withColumn("next_running_salary", F.lead("running_salary", 1).over(w12))
    .withColumn("cumulative_running_salary_avg_2", F.avg("cumulative_running_salary").over(w13))
    .withColumn("running_budget_bucket_share", F.round((F.col("running_budget_bucket") / F.sum("running_budget_bucket").over(w14)), 4))
    .withColumn("level_sum_last_known_first_known_budget", F.sum("last_known_first_known_budget").over(w15))
)

latest = (
    metrics_employees
    .withColumn("dedup_rank", F.rank().over(w16))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched_employees = (
    latest
    .select(
        "*",
        F.ntile(10).over(w17).alias("next_salary_rank_bucket"),
        F.lag("cumulative_running_salary_avg_2", 1).over(w18).alias("prev_cumulative_running_salary_avg_2"),
        F.sum("salary_rank").over(w19).alias("cumulative_salary_rank"),
    )
)

daily = (
    enriched_employees
    .groupBy(
        F.when(F.col("running_budget_bucket") >= 9, "HIGH").otherwise("LOW").alias("running_budget_bucket_tier"),
        F.date_trunc("WEEK", "hire_date").alias("week_start"),
        "dept_id",
    )
    .agg(
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_count"),
        F.countDistinct("job_title").alias("distinct_job_titles"),
    )
    .withColumn("finance_count_percent_rank", F.percent_rank().over(w20))
)

result = (
    daily
    .select(
        "running_budget_bucket_tier",
        "week_start",
        "dept_id",
        "distinct_job_titles",
    )
    .orderBy(F.col("running_budget_bucket_tier").desc())
)
