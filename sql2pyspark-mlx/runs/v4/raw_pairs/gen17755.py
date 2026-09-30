from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("job_title").orderBy("is_remote_flag", "id").rowsBetween(-2, -1)
w_running = Window.orderBy(F.col("salary_whole").desc_nulls_first())
w3 = Window.partitionBy("dept_id").orderBy("salary_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("job_title").orderBy("salary_whole", "id")
w5 = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy("level").orderBy("salary_whole_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.orderBy("running_salary_whole", "id")
w9 = Window.partitionBy("dept_id").orderBy(F.col("salary_whole").asc_nulls_first())
w10 = Window.partitionBy("dept_id").orderBy("salary_whole_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("dept_id", "level").orderBy("salary_whole_bucket", "id").rowsBetween(-6, Window.currentRow)
w12 = Window.partitionBy("dept_id").orderBy(F.col("salary_whole_percent_rank").desc(), "id")
w13 = Window.partitionBy("level")
w14 = Window.partitionBy("dept_id").orderBy("salary_whole_bucket", "id")

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-04-29").cast("date")) & F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.floor("salary").alias("salary_whole"),
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.coalesce("level", F.lit("OTHER")).alias("resolved_level"),
    )
)

with_windows = (
    cleaned
    .withColumn("salary_avg_2_prior", F.avg("salary").over(w_part))
    .withColumn("salary_whole_cume_dist", F.cume_dist().over(w_running))
)

with_windows_employees = (
    with_windows
    .withColumn("running_salary_whole", F.sum("salary_whole").over(w3))
    .withColumn("salary_whole_bucket", F.ntile(5).over(w4))
    .withColumn("salary_avg_5", F.avg("salary").over(w5))
)

base = (
    with_windows_employees
    .filter(F.col("hire_date") > F.to_date(F.lit("2023-09-06")))
    .select(
        "id",
        "email",
        "dept_id",
        "hire_date",
        "is_remote",
        "level",
        "salary_whole",
        "is_remote_flag",
        "salary_avg_2_prior",
        "salary_whole_cume_dist",
        "running_salary_whole",
        "salary_whole_bucket",
        "salary_avg_5",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.when(F.col("salary_whole_bucket") >= 7, "High").when(F.col("salary_whole_bucket") >= 3, "Medium").otherwise("Low").alias("salary_whole_bucket_level"),
    )
)

with_windows_employees_2 = (
    base
    .select(
        "*",
        F.sum("salary_avg_5").over(w6).alias("cumulative_salary_avg_5"),
        F.avg("salary_whole").over(w7).alias("salary_whole_avg_2"),
        F.row_number().over(w8).alias("event_seq"),
        F.percent_rank().over(w9).alias("salary_whole_percent_rank"),
        F.first("salary_avg_5", ignorenulls=True).over(w10).alias("first_known_salary_avg_5"),
    )
)

with_windows_employees_3 = (
    with_windows_employees_2
    .withColumn("running_salary_whole_avg_6", F.avg("running_salary_whole").over(w11))
    .withColumn("row_seq", F.row_number().over(w12))
    .withColumn("cumulative_salary_avg_5_share", F.round((F.col("cumulative_salary_avg_5") / F.sum("cumulative_salary_avg_5").over(w13)), 4))
    .withColumn("salary_whole_bucket_bucket", F.ntile(5).over(w14))
)

enriched = (
    with_windows_employees_3
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.location"))
)

result = (
    enriched
    .select(
        "id",
        "hire_date",
        "level",
        "salary_whole",
        "is_remote_flag",
        "salary_avg_2_prior",
        "salary_whole_cume_dist",
        "salary_whole_bucket",
        "salary_avg_5",
    )
    .orderBy(F.col("salary_whole").desc())
)
