from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.orderBy(F.col("salary").desc(), "id")
w3 = Window.partitionBy("dept_id", "job_title")
w4 = Window.partitionBy("job_title").orderBy("salary_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_id").orderBy(F.col("salary_whole").asc_nulls_last())
w6 = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.partitionBy("level").orderBy("running_salary_whole", "id").rowsBetween(-2, -1)
w8 = Window.partitionBy("manager_id", "job_title").orderBy("dept_id_max_salary_whole_avg_6", "id")
w9 = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("job_title", "dept_id").orderBy("salary_whole_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("salary_whole", "id")

base = (
    df_employees
    .filter(F.col("dept_id") == 31)
    .select(
        "*",
        F.ceil("salary").alias("salary_whole"),
        F.coalesce("job_title", F.lit("N/A")).alias("resolved_job_title"),
    )
)

deduped = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    deduped
    .select(
        "*",
        F.ntile(3).over(w2).alias("salary_bucket"),
        F.max("salary_whole").over(w3).alias("dept_id_max_salary_whole"),
        F.sum("salary_whole").over(w4).alias("running_salary_whole"),
        F.cume_dist().over(w5).alias("salary_whole_cume_dist"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.avg("dept_id_max_salary_whole").over(w6).alias("dept_id_max_salary_whole_avg_6"),
        F.avg("running_salary_whole").over(w7).alias("running_salary_whole_avg_2_prior"),
    )
)

enriched_employees = (
    metrics
    .withColumn("next_dept_id_max_salary_whole_avg_6", F.lead("dept_id_max_salary_whole_avg_6", 2).over(w8))
    .withColumn("cumulative_salary_bucket", F.sum("salary_bucket").over(w9))
    .withColumn("last_known_salary_whole", F.last("salary_whole", ignorenulls=True).over(w_running))
    .withColumn("event_seq", F.row_number().over(w11))
)

with_dims = (
    enriched_employees
    .alias("e")
    .join(df_employees.alias("b"), F.col("e.manager_id") == F.col("b.id"), "inner")
    .select(
        "e.*",
        F.col("b.level").alias("employee_level"),
        F.col("b.full_name").alias("employee_full_name"),
        F.col("b.hire_date").alias("employee_hire_date"),
    )
)

result = (
    with_dims
    .select(
        "id",
        "full_name",
        "job_title",
        "hire_date",
        "level",
        "salary_whole",
        "resolved_job_title",
        "salary_bucket",
        "running_salary_whole",
        F.when(F.col("job_title").rlike("^(Designer|Analyst)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("job_title_zone"),
    )
    .orderBy(F.col("level").desc_nulls_last())
)
