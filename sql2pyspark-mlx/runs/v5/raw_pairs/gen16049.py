from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_first(), "id")
w2 = Window.partitionBy("location")
w3 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_last())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("job_title").orderBy("employee_hire_date", "id").rowsBetween(-2, Window.currentRow)
w7 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-5, -1)

latest = (
    df_departments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    latest
    .filter(F.col("dept_name").isin(["Sales", "HR"]))
    .select(
        "*",
        F.count("budget").over(w2).alias("location_count_budget"),
        F.sum("budget").over(w3).alias("running_budget"),
        F.rank().over(w4).alias("budget_rank"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.location",
        "s.budget",
        F.col("c.hire_date").alias("employee_hire_date"),
        F.col("c.job_title"),
    )
)

enriched = (
    with_last
    .select(
        "*",
        F.sum("budget").over(w_part).alias("last3_sum_budget"),
        F.max("budget").over(w_part).alias("last3_max_budget"),
        F.avg("budget").over(w_part).alias("last3_avg_budget"),
        F.count("budget").over(w_part).alias("last3_count_budget"),
        F.avg("budget").over(w7).alias("budget_avg_5_prior"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("budget_avg_5_prior") >= 4429439, "LARGE").otherwise("SMALL").alias("budget_avg_5_prior_tier"),
    )
    .agg(
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("location") == "Tokyo", F.col("last3_max_budget")).otherwise(0)).alias("tokyo_last3_max_budget"),
        F.percentile_approx("budget_avg_5_prior", 0.9).alias("budget_avg_5_prior_p90"),
        F.round(F.sum("budget_avg_5_prior") / F.nullif(F.count(F.when(F.col("job_title") == "Intern", 1)), F.lit(0)), 2).alias("budget_avg_5_prior_per_intern"),
        F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"),
    )
    .filter(F.col("budget_avg_5_prior_p90") > 0)
)
