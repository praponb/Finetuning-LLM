from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("location", "dept_name").orderBy("next_budget", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("employee_job_title").orderBy(F.col("next_budget").desc_nulls_last())
w6 = Window.partitionBy("employee_job_title").orderBy("budget_avg_5_prior", "id").rowsBetween(-2, Window.currentRow)
w7 = Window.partitionBy("employee_job_title").orderBy("last7_count_budget_avg_5_prior", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.partitionBy("employee_job_title").orderBy("last7_sum_budget_avg_5_prior", "id").rowsBetween(-3, -1)
w9 = Window.partitionBy("employee_job_title")
w10 = Window.partitionBy("employee_job_title").orderBy("next_budget", "id")
w11 = Window.partitionBy("last7_sum_budget_avg_5_prior_tier").orderBy(F.col("intern_next_budget").desc())

scored = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("next_budget", F.lead("budget").over(w))
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("budget_avg_5_prior", F.avg("budget").over(w2))
)

scored_departments = (
    scored
    .select(
        "id",
        "next_budget",
        "row_seq",
        "budget_avg_5_prior",
        F.min("budget_avg_5_prior").over(w3).alias("last7_min_budget_avg_5_prior"),
        F.count("budget_avg_5_prior").over(w3).alias("last7_count_budget_avg_5_prior"),
        F.sum("budget_avg_5_prior").over(w3).alias("last7_sum_budget_avg_5_prior"),
        F.max("budget_avg_5_prior").over(w3).alias("last7_max_budget_avg_5_prior"),
    )
)

eligible = (
    scored_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.job_title") == "Intern"), "left_semi")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title").alias("employee_job_title"))
)

scored_departments_2 = (
    latest_match
    .select(
        "*",
        F.dense_rank().over(w5).alias("next_budget_dense_rank"),
        F.avg("budget_avg_5_prior").over(w6).alias("budget_avg_5_prior_avg_2"),
        F.avg("last7_count_budget_avg_5_prior").over(w7).alias("last7_count_budget_avg_5_prior_avg_6"),
    )
)

with_windows = (
    scored_departments_2
    .withColumn("budget_avg_5_prior_avg_2_avg_3_prior", F.avg("budget_avg_5_prior_avg_2").over(w8))
    .withColumn("last7_min_budget_avg_5_prior_share", F.round((F.col("last7_min_budget_avg_5_prior") / F.sum("last7_min_budget_avg_5_prior").over(w9)), 4))
    .withColumn("row_seq_2", F.row_number().over(w10))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("last7_sum_budget_avg_5_prior") >= 20348000, "PREMIUM").otherwise("STANDARD").alias("last7_sum_budget_avg_5_prior_tier"),
        F.when(F.col("last7_min_budget_avg_5_prior") >= 4361000, "LARGE").otherwise("SMALL").alias("last7_min_budget_avg_5_prior_tier"),
    )
    .agg(
        F.countDistinct("employee_job_title").alias("distinct_employee_job_titles"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("employee_job_title") == "Intern", F.col("next_budget")).otherwise(0)).alias("intern_next_budget"),
    )
    .withColumn("intern_next_budget_rank", F.rank().over(w11))
)
