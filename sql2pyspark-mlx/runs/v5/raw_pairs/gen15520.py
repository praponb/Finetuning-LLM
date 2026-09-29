from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())
w_ordered = Window.partitionBy("level")
w_recent = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("level_stddev_samp_salary", "id")
w5 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("level").orderBy(F.col("level_stddev_samp_salary").desc())
w7 = Window.orderBy("running_salary", "id")
w8 = Window.partitionBy("location").orderBy("level_stddev_samp_salary", "id").rowsBetween(-2, -1)
w9 = Window.partitionBy("location")

scored = (
    df_employees
    .select(
        "*",
        F.rank().over(w).alias("salary_rank"),
        F.stddev_samp("salary").over(w_ordered).alias("level_stddev_samp_salary"),
        F.round((F.col("salary") / F.sum("salary").over(w_ordered)), 4).alias("salary_share"),
        F.sum("salary").over(w_recent).alias("running_salary"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "email",
        "dept_id",
        "job_title",
        "is_remote",
        "level",
        "salary_rank",
        "level_stddev_samp_salary",
        "salary_share",
        "running_salary",
        F.lead("salary_share", 1).over(w4).alias("next_salary_share"),
        F.avg("running_salary").over(w5).alias("running_salary_avg_6_prior"),
        F.percent_rank().over(w6).alias("level_stddev_samp_salary_percent_rank"),
    )
)

enriched = (
    metrics
    .alias("s")
    .join(df_departments.alias("p"), F.col("s.dept_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.dept_id",
        "s.job_title",
        "s.level",
        "s.level_stddev_samp_salary",
        "s.salary_share",
        "s.running_salary",
        "s.running_salary_avg_6_prior",
        "s.level_stddev_samp_salary_percent_rank",
        F.col("p.location"),
        F.col("p.dept_name"),
    )
)

enriched_employees = (
    enriched
    .filter(F.col("running_salary_avg_6_prior") <= 469925)
    .select(
        "id",
        "job_title",
        "salary_share",
        "running_salary",
        "level_stddev_samp_salary_percent_rank",
        "location",
        F.lead("level_stddev_samp_salary_percent_rank", 1).over(w7).alias("next_level_stddev_samp_salary_percent_rank"),
        F.avg("level_stddev_samp_salary_percent_rank").over(w8).alias("level_stddev_samp_salary_percent_rank_avg_2_prior"),
        F.count("level_stddev_samp_salary").over(w9).alias("location_count_level_stddev_samp_salary"),
    )
)

result = (
    enriched_employees
    .groupBy("job_title")
    .agg(
        F.round(F.avg("level_stddev_samp_salary_percent_rank_avg_2_prior"), 2).alias("avg_level_stddev_samp_salary_percent_rank_avg_2_prior"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.countDistinct("location").alias("distinct_locations"),
        F.max("level_stddev_samp_salary_percent_rank").alias("max_level_stddev_samp_salary_percent_rank"),
        F.max("running_salary").alias("max_running_salary"),
    )
    .orderBy(F.col("distinct_locations").desc())
)
