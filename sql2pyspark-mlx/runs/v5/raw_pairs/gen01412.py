from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_first(), "id")

filtered = (
    df_departments
    .filter(F.col("location").isNull())
    .withColumn("location_group", F.when(F.col("location").isin(["Berlin", "Bangkok"]), "Core").otherwise("Other"))
    .withColumn("clean_dept_name", F.trim("dept_name"))
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.salary") <= 144000), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.dept_id").isin([28, 30])), "left_anti")
)

latest_match = (
    kept
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.job_title").alias("employee_job_title"),
        F.col("c.level").alias("employee_level"),
    )
)

latest = (
    latest_match
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .groupBy(
        F.when(F.col("budget") >= 4421000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("budget", 0.95).alias("budget_p95"),
    )
    .filter(F.col("budget_p95") >= 0)
    .orderBy(F.col("budget_tier").desc())
)
