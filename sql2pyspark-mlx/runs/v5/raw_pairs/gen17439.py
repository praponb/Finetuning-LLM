from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id")
w2 = Window.orderBy("salary", "id")
w3 = Window.partitionBy("level").orderBy("salary", "id")
w4 = Window.orderBy("latest_rank", "id")
w5 = Window.partitionBy("dept_id", "job_title").orderBy("salary", "id")
w6 = Window.partitionBy("manager_id", "dept_id").orderBy("salary_bucket_2", "id")
w7 = Window.orderBy(F.col("latest_rank").desc(), "id")
w8 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)
w9 = Window.partitionBy("job_title", "level")
w10 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .withColumn("level_lower", F.lower("level"))
)

latest = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    latest
    .select(
        "*",
        F.ntile(3).over(w2).alias("salary_bucket"),
        F.row_number().over(w3).alias("event_seq"),
        F.lag("salary", 2).over(w4).alias("prev_salary"),
        F.ntile(3).over(w5).alias("salary_bucket_2"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        "level_lower",
        "latest_rank",
        "salary_bucket",
        "salary_bucket_2",
        F.ntile(4).over(w6).alias("salary_bucket_2_bucket"),
        F.row_number().over(w7).alias("seq_no"),
        F.avg("salary").over(w8).alias("salary_avg_3"),
        F.round((F.col("salary_bucket") / F.sum("salary_bucket").over(w9)), 4).alias("salary_bucket_share"),
        F.sum("prev_salary").over(w10).alias("running_prev_salary"),
    )
)

enriched_employees = (
    enriched
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("d.location").isin(["Chiang Mai", "Singapore"]))
    .select(
        "s.id",
        "s.email",
        "s.dept_id",
        "s.manager_id",
        "s.hire_date",
        "s.seq_no",
        "s.salary_avg_3",
        "s.salary_bucket_share",
        "s.running_prev_salary",
        F.col("d.budget"),
    )
)

filtered = (
    enriched_employees
    .filter(F.col("running_prev_salary") >= 655719)
    .select(
        "id",
        "email",
        "manager_id",
        "hire_date",
        "seq_no",
        "salary_bucket_share",
        "budget",
        F.date_trunc("year", "hire_date").alias("hire_date_period"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("salary_bucket_share") >= 0.78, "PREMIUM").otherwise("STANDARD").alias("salary_bucket_share_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("budget", 0.5).alias("budget_p50"),
        F.max("hire_date_period").alias("last_hire_date_period"),
        F.min("hire_date_period").alias("first_hire_date_period"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
    )
)
