from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("job_title").orderBy(F.col("running_min_salary").desc(), "id")
w3 = Window.partitionBy("job_title")
w4 = Window.partitionBy("dept_id").orderBy("running_min_salary_bucket", "id")
w_recent = Window.partitionBy("dept_id", "dept_name").orderBy(F.col("running_min_salary_bucket").desc())
w6 = Window.orderBy("running_min_salary_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("running_min_salary_bucket", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.partitionBy("dept_name").orderBy("running_max_salary_share", "id")

with_windows = (
    df_employees
    .withColumn("running_sum_salary", F.sum("salary").over(w))
    .withColumn("running_max_salary", F.max("salary").over(w))
    .withColumn("running_min_salary", F.min("salary").over(w))
)

filtered = (
    with_windows
    .filter(F.col("job_title").isin(["Analyst", "Engineer"]))
    .withColumn("job_title_title", F.initcap(F.trim("job_title")))
    .withColumn("job_title_filled", F.coalesce("job_title", F.lit("N/A")))
)

enriched = (
    filtered
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "running_min_salary",
        "job_title_filled",
        F.ntile(3).over(w2).alias("running_min_salary_bucket"),
        F.round((F.col("running_max_salary") / F.sum("running_max_salary").over(w3)), 4).alias("running_max_salary_share"),
    )
)

enriched_employees = (
    enriched
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.location") == "New York")
    .select(
        "s.id",
        "s.dept_id",
        "s.salary",
        "s.hire_date",
        "s.is_remote",
        "s.running_min_salary",
        "s.job_title_filled",
        "s.running_min_salary_bucket",
        "s.running_max_salary_share",
        F.col("d.dept_name"),
    )
)

scored = (
    enriched_employees
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("running_min_salary_bucket_percent_rank", F.percent_rank().over(w_recent))
    .withColumn("cumulative_running_min_salary_bucket", F.sum("running_min_salary_bucket").over(w6))
    .withColumn("running_max_salary_share_avg_6", F.avg("running_max_salary_share").over(w7))
    .withColumn("prev_salary", F.lag("salary", 2).over(w8))
)

enriched_employees_2 = (
    scored
    .alias("a")
    .join(df_departments.alias("b"), F.col("a.dept_id") == F.col("b.id"), "left")
    .select(
        "a.*",
        F.col("b.location"),
        F.col("b.budget"),
        F.col("b.dept_name").alias("department_dept_name"),
    )
)

result = (
    enriched_employees_2
    .select(
        "id",
        "dept_id",
        "salary",
        "hire_date",
        "is_remote",
        "running_min_salary",
        "running_min_salary_bucket",
        "running_max_salary_share",
        "dept_name",
        F.when(F.col("dept_name").rlike("^(Finance|Engineering)$"), "GROUP_B").when(F.col("dept_name").rlike("^(Sales|Marketing)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("dept_name_zone"),
    )
    .orderBy(
        F.col("running_min_salary_bucket").desc_nulls_first(),
        "running_max_salary_share",
    )
)
