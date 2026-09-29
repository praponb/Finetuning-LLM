from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title")
w_running = Window.partitionBy("level", "job_title").orderBy("salary", "id")
w3 = Window.partitionBy("dept_id", "manager_id")
w4 = Window.partitionBy("level", "dept_id")
w5 = Window.partitionBy("dept_id").orderBy("salary", "id")
w6 = Window.partitionBy("manager_id")
w_hist = Window.partitionBy("manager_id", "level").orderBy(F.col("dedup_rank").desc(), "id")
w8 = Window.partitionBy("manager_id").orderBy("job_title_stddev_samp_salary", "id")
w9 = Window.orderBy("job_title_stddev_samp_salary", "id")
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.sum("salary").alias("sum_employee_salary"))

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-03-23").cast("date")) & ~F.col("is_remote"))
    .withColumn("level_group", F.when(F.col("level").isin(["L5", "L4"]), "Priority").otherwise("Other"))
)

base = (
    cleaned
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-06-15"))) & (~F.col("job_title").eqNullSafe(F.lit("Analyst"))))
    .select("*", F.add_months("hire_date", 1).alias("hire_date_plus_months"))
)

metrics = (
    base
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "level_group",
        F.stddev_samp("salary").over(w).alias("job_title_stddev_samp_salary"),
        F.ntile(4).over(w_running).alias("salary_bucket"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share"),
        F.count("salary").over(w4).alias("level_count_salary"),
    )
)

deduped = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped
    .select(
        "*",
        F.min("salary_bucket").over(w6).alias("manager_id_min_salary_bucket"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.ntile(10).over(w8).alias("job_title_stddev_samp_salary_bucket"),
        F.ntile(5).over(w9).alias("job_title_stddev_samp_salary_bucket_2"),
    )
)

totals = (
    enriched
    .groupBy(
        "manager_id",
        F.when(F.col("level_count_salary") >= 18, "HIGH").otherwise("LOW").alias("level_count_salary_tier"),
    )
    .agg(
        F.size(F.collect_set("level")).alias("num_levels"),
        F.sum(F.when(F.col("level") == "L3", F.col("level_count_salary")).otherwise(0)).alias("l3_level_count_salary"),
        F.sum(F.when(F.col("level") == "L4", F.col("job_title_stddev_samp_salary_bucket_2")).otherwise(0)).alias("l4_job_title_stddev_samp_salary_bucket_2"),
        F.size(F.collect_set("level")).alias("num_levels_2"),
        F.avg(F.when(F.col("level") == "L4", F.col("job_title_stddev_samp_salary_bucket_2"))).alias("l4_avg_job_title_stddev_samp_salary_bucket_2"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    totals
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "manager_id",
        "level_count_salary_tier",
        "num_levels",
        "l4_job_title_stddev_samp_salary_bucket_2",
        "num_levels_2",
        "l4_avg_job_title_stddev_samp_salary_bucket_2",
        F.col("sum_employee_salary"),
    )
    .orderBy(F.col("num_levels_2").desc())
)
