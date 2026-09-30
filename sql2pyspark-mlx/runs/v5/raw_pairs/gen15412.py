from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("budget").desc(), "id")
w4 = Window.orderBy(F.col("cumulative_budget").desc_nulls_first())
w5 = Window.orderBy("budget_bucket", "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & F.col("location").isin(["London", "Berlin", "Bangkok"]))
    .select(
        "*",
        F.coalesce("location", F.lit("N/A")).alias("location_filled"),
        F.lower("dept_name").alias("dept_name_lower"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "dept_name",
        "location",
        F.sum("budget").over(w).alias("location_sum_budget"),
        F.sum("budget").over(w2).alias("cumulative_budget"),
        F.ntile(3).over(w3).alias("budget_bucket"),
    )
)

scored = (
    enriched
    .withColumn("cumulative_budget_rank", F.rank().over(w4))
    .withColumn("budget_bucket_bucket", F.ntile(5).over(w5))
)

eligible = (
    scored
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (~F.col("ch.is_remote")), "left_semi")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location_sum_budget",
        "s.budget_bucket",
        F.col("c.job_title").alias("employee_job_title"),
    )
)

result = (
    with_latest
    .groupBy("employee_job_title")
    .agg(
        F.count(F.when(F.col("employee_job_title") == "Director", 1)).alias("director_rows"),
        F.count_if(F.col("employee_job_title") == "Engineer").alias("engineer_events"),
        F.sort_array(F.collect_set("employee_job_title")).alias("employee_job_title_list"),
    )
    .orderBy(F.col("engineer_events").desc())
)
