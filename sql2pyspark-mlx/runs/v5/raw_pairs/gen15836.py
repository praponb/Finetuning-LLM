from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("location", "employee_job_title").orderBy("budget", "id")
w3 = Window.partitionBy("employee_job_title", "location")
w4 = Window.partitionBy("dept_name")
w5 = Window.partitionBy("budget_share_2_tier").orderBy(F.col("tokyo_events").desc())

valid = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("location_dept_name_key", F.concat_ws("-", "location", "dept_name"))
)

valid_departments = (
    valid
    .filter(F.col("dept_name").isNotNull())
    .withColumn("dept_name_group", F.when(F.col("dept_name").isin(["Engineering", "Marketing"]), "Tier 1").otherwise("Other"))
    .withColumn("resolved_location", F.coalesce("location", F.lit("OTHER")))
)

latest_match = (
    valid_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title").alias("employee_job_title"))
)

ranked = (
    latest_match
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    ranked
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w3)), 4))
    .withColumn("budget_share_2", F.round((F.col("budget") / F.sum("budget").over(w4)), 4))
)

daily = (
    scored
    .groupBy(
        F.when(F.col("budget_share_2") >= 0.71, "LARGE").otherwise("SMALL").alias("budget_share_2_tier"),
        "employee_job_title",
    )
    .agg(
        F.percentile_approx("budget_share", 0.9).alias("budget_share_p90"),
        F.sum(F.when(F.col("location") == "New York", F.col("budget")).otherwise(0)).alias("new_york_budget"),
        F.percentile_approx("budget_share_2", 0.5).alias("budget_share_2_p50"),
        F.sort_array(F.collect_set("employee_job_title")).alias("employee_job_title_list"),
        F.count_if(F.col("location") == "Tokyo").alias("tokyo_events"),
    )
    .withColumn("tokyo_events_rank", F.rank().over(w5))
)

result = (
    daily
    .select(
        "budget_share_2_tier",
        "employee_job_title",
        "budget_share_p90",
        "new_york_budget",
        "budget_share_2_p50",
    )
    .orderBy("budget_share_2_tier")
)
