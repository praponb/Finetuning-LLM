from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_name", "location")
w_recent = Window.orderBy(F.col("first_known_budget").asc_nulls_last())
w4 = Window.partitionBy("dept_name").orderBy(F.col("first_known_budget").asc_nulls_first())
w5 = Window.partitionBy("dept_name").orderBy("first_known_budget", "id")
w6 = Window.partitionBy("dept_name").orderBy(F.col("first_known_budget").desc_nulls_first())
w7 = Window.orderBy("budget_share", "id").rowsBetween(-5, -1)

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select("*", F.coalesce("dept_name", F.lit("unassigned")).alias("dept_name_filled"))
)

with_windows = (
    filtered
    .filter(F.col("dept_name").isNotNull())
    .select(
        "id",
        "dept_name",
        "dept_name_filled",
        F.first("budget", ignorenulls=True).over(w).alias("first_known_budget"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "first_known_budget",
        "budget_share",
        F.dense_rank().over(w_recent).alias("first_known_budget_dense_rank"),
        F.percent_rank().over(w4).alias("first_known_budget_percent_rank"),
        F.lag("budget_share", 1).over(w5).alias("prev_budget_share"),
        F.cume_dist().over(w6).alias("first_known_budget_cume_dist"),
        F.avg("budget_share").over(w7).alias("budget_share_avg_5_prior"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.hire_date") <= "2025-04-01"), "left_anti")
)

result = (
    kept
    .groupBy(
        F.when(F.col("first_known_budget_dense_rank") >= 6, "HIGH").otherwise("LOW").alias("first_known_budget_dense_rank_tier"),
        F.when(F.col("first_known_budget_cume_dist") >= 0.6, "LARGE").otherwise("SMALL").alias("first_known_budget_cume_dist_tier"),
    )
    .agg(
        F.percentile_approx("first_known_budget_cume_dist", [0.25, 0.5, 0.75]).alias("first_known_budget_cume_dist_quartiles"),
        F.round(F.avg("budget_share_avg_5_prior"), 2).alias("avg_budget_share_avg_5_prior"),
        F.percentile_approx("first_known_budget_dense_rank", 0.5).alias("first_known_budget_dense_rank_p50"),
    )
    .orderBy("first_known_budget_cume_dist_tier")
)
