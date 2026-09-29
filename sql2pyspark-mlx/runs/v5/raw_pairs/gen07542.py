from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("location").orderBy("budget", "id")
w_grp = Window.partitionBy("dept_name").orderBy("running_max_budget", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (~F.col("location").eqNullSafe(F.lit("Berlin"))))
    .select(
        "*",
        F.lower("location").alias("location_lower"),
        F.when(F.col("budget") >= 4225000, "High").when(F.col("budget") >= 1353000, "Medium").otherwise("Low").alias("budget_level"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.salary") < 98000), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date").alias("employee_hire_date"), F.col("c.salary"))
)

enriched = (
    latest_match
    .filter(F.col("salary") < 92000)
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "budget_level",
        F.max("budget").over(w_hist).alias("running_max_budget"),
        F.avg("budget").over(w_hist).alias("running_avg_budget"),
        F.min("budget").over(w_hist).alias("running_min_budget"),
        F.ntile(5).over(w3).alias("budget_bucket"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.lead("running_min_budget").over(w_grp).alias("next_running_min_budget"),
        F.lag("running_min_budget").over(w_grp).alias("prev_running_min_budget"),
        F.lead("running_max_budget").over(w_grp).alias("next_running_max_budget"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)

valid = (
    with_windows
    .filter(F.col("location").isNotNull() & (F.col("dept_name").eqNullSafe(F.lit("Finance"))))
    .select(
        "id",
        "dept_name",
        "location",
        "budget_level",
        "running_max_budget",
        "running_avg_budget",
        "running_min_budget",
        "budget_bucket",
        "next_running_min_budget",
        "prev_running_min_budget",
        "seq_no",
        F.round((F.col("running_min_budget") / F.col("running_avg_budget")), 2).alias("running_min_budget_per_running_avg_budget"),
        (F.col("seq_no") + F.col("next_running_max_budget")).alias("seq_no_plus_next_running_max_budget"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("dept_name_clean"),
    )
)

latest_match_departments = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level").alias("employee_level"), F.col("c.hire_date"))
)

result = (
    latest_match_departments
    .groupBy(
        F.when(F.col("budget_bucket") >= 7, "LARGE").otherwise("SMALL").alias("budget_bucket_tier"),
        "location",
        F.date_trunc("YEAR", "hire_date").alias("year_start"),
    )
    .agg(
        F.count(F.when(F.col("employee_level") == "L1", 1)).alias("l1_rows"),
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_count"),
        F.count_if(F.col("employee_level") == "L5").alias("l5_events"),
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("dept_name") == "Engineering", F.col("running_min_budget_per_running_avg_budget"))).alias("engineering_total_running_min_budget_per_running_avg_budget"),
    )
    .orderBy(F.col("year_start").desc())
)
