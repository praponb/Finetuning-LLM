from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("salary").desc(), "id")
w_grp = Window.partitionBy("job_title", "dept_id").orderBy(F.col("hire_date").desc(), "id")
w3 = Window.partitionBy("dept_id").orderBy("salary_bucket", "id")
w4 = Window.partitionBy("job_title", "level").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_id").orderBy("salary_bucket_minus_row_seq", "id")
w_part = Window.partitionBy("manager_id").orderBy("next_salary_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("job_title", "manager_id").orderBy("next_salary_bucket_minus_row_seq", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.partitionBy("manager_id")
w_hist = Window.partitionBy("dept_id").orderBy(F.col("salary_bucket_minus_row_seq").desc(), "id")
w10 = Window.partitionBy("dept_id").orderBy("next_next_salary_bucket_minus_row_seq", "id")
w11 = Window.partitionBy("job_title", "dept_id").orderBy("salary_bucket_minus_row_seq", "id").rowsBetween(-5, Window.currentRow)
w12 = Window.partitionBy("job_title")
w13 = Window.partitionBy("job_title").orderBy("prev_salary_bucket_minus_row_seq_avg_2", "id").rowsBetween(-2, -1)
w14 = Window.partitionBy("dept_id").orderBy("prev_salary_bucket_minus_row_seq_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-08-04").cast("date"))
    .withColumn("full_name_slug", F.regexp_replace("full_name", " ", ""))
)

with_windows = (
    cleaned
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "level",
        F.ntile(5).over(w).alias("salary_bucket"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)

filtered = (
    with_windows
    .filter(F.col("job_title").isNotNull())
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "level",
        "salary_bucket",
        "row_seq",
        (F.col("salary_bucket") - F.col("row_seq")).alias("salary_bucket_minus_row_seq"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "salary_bucket",
        "salary_bucket_minus_row_seq",
        F.lead("salary_bucket", 2).over(w3).alias("next_salary_bucket"),
        F.sum("salary_bucket_minus_row_seq").over(w4).alias("running_salary_bucket_minus_row_seq"),
        F.lead("salary_bucket_minus_row_seq", 2).over(w5).alias("next_salary_bucket_minus_row_seq"),
    )
)

enriched = (
    metrics
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.budget"))
)

with_windows_employees = (
    enriched
    .select(
        "id",
        "dept_id",
        "job_title",
        "salary_bucket_minus_row_seq",
        "budget",
        F.lead("next_salary_bucket_minus_row_seq", 1, 0).over(w3).alias("next_next_salary_bucket_minus_row_seq"),
        F.sum("salary_bucket").over(w_part).alias("cumulative_salary_bucket"),
        F.avg("salary_bucket_minus_row_seq").over(w7).alias("salary_bucket_minus_row_seq_avg_2"),
        F.round((F.col("next_salary_bucket") / F.sum("next_salary_bucket").over(w8)), 4).alias("next_salary_bucket_share"),
        F.ntile(4).over(w_hist).alias("salary_bucket_minus_row_seq_bucket"),
    )
)

scored = (
    with_windows_employees
    .select(
        "*",
        F.lead("salary_bucket_minus_row_seq_bucket").over(w10).alias("next_salary_bucket_minus_row_seq_bucket"),
        F.lag("salary_bucket_minus_row_seq_bucket").over(w10).alias("prev_salary_bucket_minus_row_seq_bucket"),
        F.lag("salary_bucket_minus_row_seq_avg_2").over(w10).alias("prev_salary_bucket_minus_row_seq_avg_2"),
        F.row_number().over(w10).alias("row_seq"),
    )
)

result = (
    scored
    .select(
        "id",
        "job_title",
        "budget",
        "cumulative_salary_bucket",
        "salary_bucket_minus_row_seq_avg_2",
        "salary_bucket_minus_row_seq_bucket",
        F.avg("budget").over(w11).alias("budget_avg_5"),
        F.stddev_samp("cumulative_salary_bucket").over(w12).alias("job_title_stddev_samp_cumulative_salary_bucket"),
        F.avg("next_salary_bucket_minus_row_seq_bucket").over(w13).alias("next_salary_bucket_minus_row_seq_bucket_avg_2_prior"),
        F.sum("cumulative_salary_bucket").over(w14).alias("cumulative_cumulative_salary_bucket"),
    )
)
