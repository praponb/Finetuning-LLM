from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

filtered = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("dept_name") == "Finance"))
    .withColumn("location_dept_name", F.concat("location", F.lit("/"), "dept_name"))
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "location", "budget")
)

base = (
    deduped
    .filter(F.col("location").isNotNull() & ~F.col("location").isin(["Tokyo", "London"]))
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
    .withColumn("location_title", F.initcap(F.trim("location")))
    .withColumn("location_clean", F.coalesce("location", F.lit("UNKNOWN")))
)

valid = (
    base
    .filter(~F.col("location").isin(["Chiang Mai", "Berlin"]))
    .select(
        "id",
        "location",
        "budget_filled",
        "location_title",
        "location_clean",
        (F.col("budget_filled") * F.col("budget")).alias("budget_filled_times_budget"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location_title",
        "s.budget_filled_times_budget",
        F.col("c.job_title"),
    )
)

result = (
    with_last
    .groupBy("job_title")
    .agg(
        F.avg(F.when(F.col("job_title") == "Intern", F.col("budget_filled_times_budget"))).alias("intern_avg_budget_filled_times_budget"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("cnt"),
        F.min("budget_filled_times_budget").alias("min_budget_filled_times_budget"),
    )
)
