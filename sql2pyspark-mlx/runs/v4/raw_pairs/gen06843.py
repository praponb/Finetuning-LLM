from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.orderBy("salary", "id")
w3 = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("dept_id")
w5 = Window.partitionBy("level").orderBy(F.col("dept_id_count_salary").desc(), "id")
w6 = Window.partitionBy("job_title").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("dept_id").orderBy("dept_id_count_salary", "id")
w8 = Window.partitionBy("job_title", "level").orderBy("salary_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("job_title").orderBy(F.col("rank_in_group_whole").asc_nulls_first())
w10 = Window.partitionBy("job_title").orderBy("hire_date", "id")
w_grp = Window.partitionBy("level").orderBy("last_known_salary_bucket_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("level", "job_title").orderBy(F.col("salary_bucket").desc(), "id")
w_grp_2 = Window.partitionBy("level").orderBy("last_known_salary_bucket", "id")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.min("salary").alias("min_employee_salary"))

deduped = (
    df_employees
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    deduped
    .withColumn("salary_bucket", F.ntile(5).over(w2))
    .withColumn("salary_avg_3_prior", F.avg("salary").over(w3))
    .withColumn("dept_id_count_salary", F.count("salary").over(w4))
)

deduped_employees = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    deduped_employees
    .select(
        "*",
        F.first("level", ignorenulls=True).over(w6).alias("first_known_level"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.last("salary_bucket", ignorenulls=True).over(w8).alias("last_known_salary_bucket"),
    )
)

cleaned = (
    scored
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-09-09"))) & F.col("manager_id").isNotNull())
    .select(
        "id",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        "rank_in_group",
        "salary_bucket",
        "dedup_rank",
        "first_known_level",
        "last_known_salary_bucket",
        F.floor("rank_in_group").alias("rank_in_group_whole"),
        F.least("dedup_rank", "salary_bucket").alias("least_dedup_rank_salary_bucket"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.round((F.col("last_known_salary_bucket") / F.sum("last_known_salary_bucket").over(w4)), 4).alias("last_known_salary_bucket_share"),
        F.rank().over(w9).alias("rank_in_group_whole_rank"),
    )
)

enriched = (
    metrics
    .withColumn("next_rank_in_group_whole_rank", F.lead("rank_in_group_whole_rank", 1, 0).over(w10))
    .withColumn("cumulative_rank_in_group_whole_rank", F.sum("rank_in_group_whole_rank").over(w_grp))
    .withColumn("salary_bucket_bucket", F.ntile(10).over(w12))
    .withColumn("rank_in_group_whole_rank_share", F.round((F.col("rank_in_group_whole_rank") / F.sum("rank_in_group_whole_rank").over(w4)), 4))
    .withColumn("next_least_dedup_rank_salary_bucket", F.lead("least_dedup_rank_salary_bucket", 2).over(w_grp_2))
)

result = (
    enriched
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "dept_id",
        "job_title",
        "hire_date",
        "level",
        "salary_bucket",
        "dedup_rank",
        "last_known_salary_bucket",
        "rank_in_group_whole",
        F.when(F.col("job_title").rlike("^(Designer|Analyst)$"), "CORE").when(F.col("job_title").rlike("^(Engineer|Manager)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("job_title_zone"),
        F.col("min_employee_salary"),
    )
    .orderBy(F.col("rank_in_group_whole").desc_nulls_first())
)
