from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "level").orderBy(F.col("is_remote_flag").asc_nulls_last())
w2 = Window.partitionBy("job_title").orderBy(F.col("is_remote_flag").desc(), "id")
w3 = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("job_title")
w5 = Window.partitionBy("dept_id").orderBy("hire_date", "id")
w6 = Window.partitionBy("dept_id", "level").orderBy(F.col("salary").desc(), "id")
w_recent = Window.orderBy("salary", "id")
w_recent_2 = Window.partitionBy("manager_id").orderBy(F.col("is_remote_flag_percent_rank").asc_nulls_last())
w9 = Window.orderBy(F.col("is_remote_flag_percent_rank_cume_dist").desc())
w10 = Window.orderBy(F.col("is_remote_flag").desc())
w11 = Window.orderBy("is_remote_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("dept_id")
w13 = Window.partitionBy("manager_id")
w_hist = Window.partitionBy("level", "dept_id")
w_running = Window.orderBy("is_remote_flag", "id")

base = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-12-20").cast("date")) & F.col("manager_id").isNotNull() & F.col("full_name").like("%bob%"))
    .withColumn("is_remote_flag", F.col("is_remote").cast("int"))
)

with_dims = (
    base
    .alias("e")
    .join(df_employees.alias("p2"), F.col("e.manager_id") == F.col("p2.id"), "left")
    .select(
        "e.*",
        F.col("p2.hire_date").alias("employee_hire_date"),
        F.col("p2.full_name").alias("employee_full_name"),
    )
)

metrics = (
    with_dims
    .select(
        "*",
        F.percent_rank().over(w).alias("is_remote_flag_percent_rank"),
        F.ntile(4).over(w2).alias("is_remote_flag_bucket"),
        F.first("is_remote_flag", ignorenulls=True).over(w3).alias("first_known_is_remote_flag"),
        F.max("is_remote_flag").over(w4).alias("job_title_max_is_remote_flag"),
        F.lag("salary", 1).over(w5).alias("prev_salary"),
    )
)

ranked = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    ranked
    .select(
        "*",
        F.ntile(4).over(w_recent).alias("salary_bucket"),
        F.cume_dist().over(w_recent_2).alias("is_remote_flag_percent_rank_cume_dist"),
    )
)

enriched = (
    scored
    .alias("a")
    .join(df_employees.alias("d"), F.col("a.manager_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.salary").alias("employee_salary"))
)

scored_employees = (
    enriched
    .select(
        "*",
        F.rank().over(w9).alias("is_remote_flag_percent_rank_cume_dist_rank"),
        F.rank().over(w10).alias("is_remote_flag_rank"),
    )
)

result = (
    scored_employees
    .withColumn("first_known_is_remote_flag_rank", F.first("is_remote_flag_rank", ignorenulls=True).over(w11))
    .withColumn("is_remote_flag_percent_rank_cume_dist_rank_share", F.round((F.col("is_remote_flag_percent_rank_cume_dist_rank") / F.sum("is_remote_flag_percent_rank_cume_dist_rank").over(w12)), 4))
    .withColumn("manager_id_avg_job_title_max_is_remote_flag", F.avg("job_title_max_is_remote_flag").over(w13))
    .withColumn("level_avg_is_remote_flag_percent_rank_cume_dist_rank", F.avg("is_remote_flag_percent_rank_cume_dist_rank").over(w_hist))
    .withColumn("prev_salary_bucket", F.lag("salary_bucket", 1).over(w_running))
)
