from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget").desc_nulls_first())
w2 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_first())

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.coalesce("location", F.lit("unassigned")).alias("location_filled"),
        F.ceil("budget").alias("budget_whole"),
    )
)

remaining_keys = (
    filtered
    .select("location", "dept_name")
    .subtract(
        df_departments
        .filter(~F.col("location").isin(["London", "Chiang Mai", "New York"]))
        .select("location", "dept_name")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.is_remote")), "left_anti")
)

filtered_departments = (
    kept
    .filter(F.col("location").isNotNull() & (F.col("dept_name").eqNullSafe(F.lit("Finance"))))
    .select(
        "dept_name",
        "location",
        "budget",
        "location_filled",
        "budget_whole",
        F.round("budget_whole", 0).alias("budget_whole_rounded"),
        F.coalesce("location", F.lit("UNKNOWN")).alias("resolved_location"),
    )
)

with_windows = (
    filtered_departments
    .filter(F.col("budget_whole") == 72)
    .select(
        "dept_name",
        "location",
        F.cume_dist().over(w).alias("budget_cume_dist"),
        F.rank().over(w2).alias("budget_rank"),
    )
)

result = (
    with_windows
    .rollup(
        F.when(F.col("budget_cume_dist") >= 0.71, "LARGE").otherwise("SMALL").alias("budget_cume_dist_tier"),
        F.when(F.col("budget_rank") >= 9, "HIGH").otherwise("LOW").alias("budget_rank_tier"),
        "dept_name",
    )
    .agg(
        F.sum("budget_rank").alias("total_budget_rank"),
        F.percentile_approx("budget_cume_dist", [0.25, 0.5, 0.75]).alias("budget_cume_dist_quartiles"),
        F.count_if(F.col("dept_name") == "Sales").alias("sales_events"),
    )
    .orderBy(F.col("budget_cume_dist_tier").desc_nulls_first())
)
