from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level", "dept_id")
w3 = Window.partitionBy("dept_id", "manager_id")
w4 = Window.partitionBy("job_title").orderBy("salary_share", "id")
w5 = Window.partitionBy("job_title").orderBy("prev_salary_share", "id")
w6 = Window.partitionBy("job_title").orderBy("prev_salary_share_2", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("job_title")
w8 = Window.partitionBy("job_title").orderBy("row_num_whole", "id").rowsBetween(-3, -1)
w9 = Window.partitionBy("job_title").orderBy("prev_salary_share_2", "id")

with_windows = (
    df_employees
    .select(
        "id",
        "email",
        "job_title",
        "level",
        F.first("level", ignorenulls=True).over(w).alias("first_known_level"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share_2"),
    )
)

filtered = (
    with_windows
    .filter(F.col("job_title").isNotNull())
    .select(
        "id",
        "job_title",
        "first_known_level",
        "salary_share",
        "salary_share_2",
        F.trim("job_title").alias("clean_job_title"),
        F.coalesce("level", F.lit("DIRECT")).alias("level_filled"),
    )
)

with_windows_employees = (
    filtered
    .select(
        "*",
        F.lead("salary_share").over(w4).alias("next_salary_share"),
        F.lag("salary_share").over(w4).alias("prev_salary_share"),
        F.lag("salary_share_2").over(w4).alias("prev_salary_share_2"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

latest = (
    with_windows_employees
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

base = (
    latest
    .filter(F.col("job_title").isNotNull())
    .select(
        "*",
        F.ceil("row_num").alias("row_num_whole"),
        F.coalesce("job_title", F.lit("OTHER")).alias("resolved_job_title"),
    )
)

scored = (
    base
    .withColumn("prev_salary_share_2_avg_5", F.avg("prev_salary_share_2").over(w6))
    .withColumn("row_seq_share", F.round((F.col("row_seq") / F.sum("row_seq").over(w7)), 4))
    .withColumn("prev_salary_share_bucket", F.ntile(5).over(w5))
    .withColumn("prev_salary_share_2_avg_3_prior", F.avg("prev_salary_share_2").over(w8))
    .withColumn("prev_prev_salary_share_2", F.lag("prev_salary_share_2", 1, 0).over(w9))
)

stats = (
    scored
    .rollup(
        F.when(F.col("prev_salary_share_bucket") >= 9, "LARGE").otherwise("SMALL").alias("prev_salary_share_bucket_tier"),
        F.when(F.col("prev_salary_share_2_avg_3_prior") >= 1, "HIGH").otherwise("LOW").alias("prev_salary_share_2_avg_3_prior_tier"),
        "job_title",
    )
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.avg("prev_salary_share_2").alias("avg_prev_salary_share_2"),
    )
    .filter(F.col("avg_prev_salary_share_2") > 0)
)

result = (
    stats
    .select(
        "prev_salary_share_bucket_tier",
        "prev_salary_share_2_avg_3_prior_tier",
        "job_title",
        "job_title_list",
        F.when(F.col("job_title").rlike("^(Intern|Designer)$"), "GROUP_B").otherwise("GROUP_A").alias("job_title_zone"),
    )
    .orderBy(F.col("prev_salary_share_2_avg_3_prior_tier").asc_nulls_last())
)
