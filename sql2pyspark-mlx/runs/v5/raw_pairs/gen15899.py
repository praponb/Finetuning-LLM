from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

filtered = (
    df_departments
    .filter(F.col("location").isNull())
    .withColumn("location_dept_name", F.concat("location", F.lit(" "), "dept_name"))
)

scored = (
    filtered
    .filter(F.col("location") != "Berlin")
    .withColumn("running_count_budget", F.count("budget").over(w))
    .withColumn("running_min_budget", F.min("budget").over(w))
    .withColumn("running_avg_budget", F.avg("budget").over(w))
    .withColumn("running_sum_budget", F.sum("budget").over(w))
    .withColumn("last_known_budget", F.last("budget", ignorenulls=True).over(w2))
)

not_flagged = (
    scored
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.email").like("%@yahoo.com")), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location_dept_name",
        "s.running_count_budget",
        "s.running_min_budget",
        "s.running_avg_budget",
        "s.running_sum_budget",
        "s.last_known_budget",
        F.col("c.job_title"),
        F.col("c.salary"),
    )
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location_dept_name",
        "s.running_count_budget",
        "s.running_min_budget",
        "s.job_title",
        "s.salary",
        F.col("c.salary").alias("employee_salary"),
    )
)

matched_keys = (
    with_latest
    .select("id")
    .subtract(
        df_departments
        .filter(F.col("budget") > 1617000)
        .select("id")
    )
)

verified = (
    with_latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

stats = (
    verified
    .groupBy(
        F.when(F.col("salary") >= 147000, "PREMIUM").otherwise("STANDARD").alias("salary_tier"),
        "job_title",
    )
    .agg(
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.countDistinct("job_title").alias("distinct_job_titles"),
        F.count_if(F.col("job_title") == "Analyst").alias("analyst_events"),
        F.size(F.collect_set("job_title")).alias("num_job_titles_2"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    stats
    .select(
        "salary_tier",
        "job_title",
        "num_job_titles",
        "distinct_job_titles",
        "analyst_events",
        "num_job_titles_2",
        "distinct_ids",
    )
)
