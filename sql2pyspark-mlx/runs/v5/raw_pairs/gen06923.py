from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id")
w2 = Window.partitionBy("manager_id", "job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_id")
w4 = Window.orderBy("rank_in_group", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-05-22").cast("date")) & (F.col("salary") >= 86000))
    .withColumn("full_name_upper", F.upper("full_name"))
    .withColumn("salary_whole", F.ceil("salary"))
    .withColumn("level_clean", F.coalesce("level", F.lit("OTHER")))
)

ranked = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    ranked
    .select(
        "id",
        "full_name",
        "dept_id",
        "salary",
        "hire_date",
        "is_remote",
        "full_name_upper",
        "level_clean",
        "rank_in_group",
        F.sum("salary_whole").over(w2).alias("running_sum_salary_whole"),
        F.min("salary_whole").over(w2).alias("running_min_salary_whole"),
        F.max("salary_whole").over(w2).alias("running_max_salary_whole"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "dept_id",
        "is_remote",
        "running_min_salary_whole",
        "running_max_salary_whole",
        F.stddev_samp("running_max_salary_whole").over(w3).alias("dept_id_stddev_samp_running_max_salary_whole"),
        F.avg("running_max_salary_whole").over(w4).alias("running_max_salary_whole_avg_3"),
    )
)

enriched = (
    with_windows
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.dept_id",
        "src.is_remote",
        "src.running_max_salary_whole",
        "src.dept_id_stddev_samp_running_max_salary_whole",
        "src.running_max_salary_whole_avg_3",
        F.col("d.location"),
        F.col("d.budget"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("dept_id_stddev_samp_running_max_salary_whole") >= 79, "HIGH").otherwise("LOW").alias("dept_id_stddev_samp_running_max_salary_whole_tier"),
        "dept_id",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("running_max_salary_whole_avg_3", [0.25, 0.5, 0.75]).alias("running_max_salary_whole_avg_3_quartiles"),
        F.percentile_approx("running_max_salary_whole_avg_3", [0.25, 0.5, 0.75]).alias("running_max_salary_whole_avg_3_quartiles_2"),
        F.percentile_approx("dept_id_stddev_samp_running_max_salary_whole", [0.25, 0.5, 0.75]).alias("dept_id_stddev_samp_running_max_salary_whole_quartiles"),
    )
    .filter(F.col("distinct_ids") < 1000)
    .orderBy(
        F.col("dept_id_stddev_samp_running_max_salary_whole_tier").asc_nulls_last(),
    )
)
