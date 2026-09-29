from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title")
w2 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.partitionBy("manager_id").orderBy(F.col("job_title_max_salary").desc(), "id")
w_recent = Window.orderBy("salary", "id").rowsBetween(-3, -1)
w_part = Window.partitionBy("level").orderBy("job_title_max_salary_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_employees
    .withColumn("job_title_max_salary", F.max("salary").over(w))
    .withColumn("last_known_level", F.last("level", ignorenulls=True).over(w2))
)

with_windows_employees = (
    with_windows
    .withColumn("last_known_job_title", F.last("job_title", ignorenulls=True).over(w3))
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w)), 4))
    .withColumn("job_title_max_salary_avg_3", F.avg("job_title_max_salary").over(w4))
    .withColumn("seq_no", F.row_number().over(w_grp))
)

filtered = (
    with_windows_employees
    .filter((F.col("hire_date") >= F.lit("2023-06-02").cast("date")) & F.col("manager_id").isNotNull())
    .select(
        "*",
        F.when(F.col("level").isin(["L3", "L5"]), "Priority").otherwise("Other").alias("level_group"),
    )
)

enriched = (
    filtered
    .withColumn("salary_share_avg_3_prior", F.avg("salary_share").over(w_recent))
    .withColumn("cumulative_job_title_max_salary_avg_3", F.sum("job_title_max_salary_avg_3").over(w_part))
)

result = (
    enriched
    .withColumn("seq_no_tier", F.when(F.col("seq_no") >= 9, "PREMIUM").otherwise("STANDARD"))
    .withColumn("job_title_max_salary_avg_3_tier", F.when(F.col("job_title_max_salary_avg_3") >= 146540, "LARGE").otherwise("SMALL"))
    .rollup("seq_no_tier", "job_title_max_salary_avg_3_tier")
    .agg(
        F.round(F.sum("salary_share") / F.nullif(F.count(F.when(F.col("level") == "L3", 1)), F.lit(0)), 2).alias("salary_share_per_l3"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.percentile_approx("cumulative_job_title_max_salary_avg_3", 0.5).alias("cumulative_job_title_max_salary_avg_3_p50"),
        F.grouping_id().alias("gid"),
    )
    .orderBy("seq_no_tier")
)
