from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "dept_id").orderBy("hire_date", "id")
w_part = Window.partitionBy("manager_id", "dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("level").orderBy("salary", "id")
w4 = Window.partitionBy("manager_id").orderBy(F.col("rank_in_group").desc(), "id")
w5 = Window.partitionBy("job_title", "level").orderBy(F.col("running_avg_salary").desc(), "id")
w6 = Window.partitionBy("job_title")
w_grp = Window.partitionBy("level", "job_title").orderBy(F.col("salary").desc())

cleaned = (
    df_employees
    .filter((F.col("salary") > 149000) & F.col("manager_id").isNotNull())
    .select(
        "*",
        F.substring("job_title", 1, 2).alias("job_title_prefix"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("level_filled"),
    )
)

latest = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.max("salary").over(w_part).alias("running_max_salary"),
        F.avg("salary").over(w_part).alias("running_avg_salary"),
        F.sum("salary").over(w_part).alias("running_sum_salary"),
        F.count("salary").over(w_part).alias("running_count_salary"),
        F.ntile(5).over(w_running).alias("salary_bucket"),
    )
)

ranked = (
    scored
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

deduped = (
    ranked
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

cleaned_employees = (
    deduped
    .filter(~F.col("is_remote"))
    .select(
        "id",
        "email",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "level_filled",
        "rank_in_group",
        "running_max_salary",
        "running_avg_salary",
        F.col("running_max_salary").cast("int").alias("running_max_salary_int"),
        F.upper("full_name").alias("full_name_upper"),
    )
)

with_windows = (
    cleaned_employees
    .select(
        "*",
        F.min("salary").over(w6).alias("job_title_min_salary"),
        F.dense_rank().over(w_grp).alias("salary_dense_rank"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("running_avg_salary") >= 156000, "PREMIUM").otherwise("STANDARD").alias("running_avg_salary_tier"),
        F.date_trunc("WEEK", "hire_date").alias("week_start"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.percentile_approx("salary", 0.95).alias("salary_p95"),
    )
    .orderBy(F.col("salary_p95").desc())
)
