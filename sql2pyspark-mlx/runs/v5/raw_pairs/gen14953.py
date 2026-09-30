from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy("is_berlin", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("budget", "id")
w4 = Window.partitionBy("location", "dept_name")
w_recent = Window.partitionBy("location").orderBy("is_berlin", "id").rowsBetween(-3, Window.currentRow)
w_recent_2 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("dept_name") != "Engineering"))
    .select(
        "*",
        F.when(F.col("budget") >= 3359000, "High").when(F.col("budget") >= 900000, "Medium").otherwise("Low").alias("budget_level"),
        F.when(F.col("location") == "Berlin", 1).otherwise(0).alias("is_berlin"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "location",
        "budget",
        "budget_level",
        "is_berlin",
        F.lead("budget", 1).over(w).alias("next_budget"),
        F.first("dept_name", ignorenulls=True).over(w2).alias("first_known_dept_name"),
        F.lead("budget", 1).over(w3).alias("next_budget_2"),
        F.max("budget").over(w4).alias("location_max_budget"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.manager_id") == 32), "left_semi")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.level").isNull()), "left_semi")
)

with_windows = (
    eligible
    .filter(F.col("budget").isNotNull())
    .select(
        "id",
        "location",
        "budget_level",
        "is_berlin",
        "next_budget",
        "next_budget_2",
        "location_max_budget",
        F.count("location_max_budget").over(w_recent).alias("last4_count_location_max_budget"),
        F.max("location_max_budget").over(w_recent).alias("last4_max_location_max_budget"),
        F.sum("location_max_budget").over(w_recent).alias("last4_sum_location_max_budget"),
        F.sum("next_budget").over(w_recent_2).alias("running_next_budget"),
    )
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location",
        "s.budget_level",
        "s.next_budget",
        "s.location_max_budget",
        "s.last4_sum_location_max_budget",
        F.col("c.job_title"),
    )
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.hire_date"),
        F.col("c.job_title").alias("employee_job_title"),
    )
)

result = (
    latest_match
    .groupBy(
        F.when(F.col("location_max_budget") >= 3919642, "PREMIUM").otherwise("STANDARD").alias("location_max_budget_tier"),
        F.date_trunc("QUARTER", "hire_date").alias("quarter_start"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum("location_max_budget") / F.nullif(F.count(F.when(F.col("location") == "Chiang Mai", 1)), F.lit(0)), 2).alias("location_max_budget_per_chiang_mai"),
    )
)
