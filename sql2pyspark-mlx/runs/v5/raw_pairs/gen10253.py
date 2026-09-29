from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")
w_hist = Window.partitionBy("location").orderBy("location_min_budget", "id")
w_recent = Window.orderBy("location_max_budget", "id")
w4 = Window.orderBy("event_seq", "id")

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("N/A")))
    .withColumn("resolved_dept_name", F.coalesce("dept_name", F.lit("OTHER")))
)

cleaned = (
    base
    .filter(F.col("budget_filled") < 57)
    .select(
        "*",
        F.initcap(F.trim("location")).alias("location_title"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("dept_name_filled_2"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "location",
        "budget",
        "dept_name_filled",
        "resolved_dept_name",
        "dept_name_filled_2",
        F.min("budget").over(w).alias("location_min_budget"),
        F.max("budget").over(w).alias("location_max_budget"),
    )
)

scored = (
    with_windows
    .withColumn("next_location_max_budget", F.lead("location_max_budget").over(w_hist))
    .withColumn("prev_budget", F.lag("budget").over(w_hist))
    .withColumn("next_budget", F.lead("budget").over(w_hist))
    .withColumn("event_seq", F.row_number().over(w_hist))
    .withColumn("next_location_max_budget_2", F.lead("location_max_budget", 1).over(w_recent))
)

scored_departments = (
    scored
    .select(
        "*",
        F.lead("prev_budget", 1).over(w4).alias("next_prev_budget"),
        F.round((F.col("event_seq") / F.sum("event_seq").over(w)), 4).alias("event_seq_share"),
    )
)

totals = (
    scored_departments
    .cube(
        "location",
        F.when(F.col("next_location_max_budget_2") >= 3582286, "LARGE").otherwise("SMALL").alias("next_location_max_budget_2_tier"),
    )
    .agg(
        F.round(F.avg("next_location_max_budget_2"), 2).alias("avg_next_location_max_budget_2"),
        F.count(F.when(F.col("location") == "New York", 1)).alias("new_york_count"),
        F.count(F.when(F.col("location") == "New York", 1)).alias("new_york_rows"),
        F.percentile_approx("next_budget", [0.25, 0.5, 0.75]).alias("next_budget_quartiles"),
    )
)

result = (
    totals
    .select(
        "location",
        "next_location_max_budget_2_tier",
        "new_york_count",
        "new_york_rows",
        "next_budget_quartiles",
    )
    .orderBy("new_york_rows")
)
