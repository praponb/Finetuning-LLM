from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("location").orderBy("budget", "id")
w_grp = Window.partitionBy("dept_name").orderBy("next_budget", "id").rowsBetween(-6, Window.currentRow)

scored = (
    df_departments
    .filter(F.col("dept_name") == "Sales")
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.lead("budget").over(w_hist).alias("next_budget"),
        F.lag("budget").over(w_hist).alias("prev_budget"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.min("budget").over(w_grp).alias("last7_min_budget"),
        F.max("budget").over(w_grp).alias("last7_max_budget"),
        F.avg("budget").over(w_grp).alias("last7_avg_budget"),
    )
)

matched_keys = (
    metrics
    .select("location", "dept_name")
    .subtract(
        df_departments
        .filter(F.col("budget").between(3427000, 3724000))
        .select("location", "dept_name")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.manager_id") == 33), "left_semi")
)

daily = (
    kept
    .groupBy(
        F.when(F.col("prev_budget") >= 3781000, "LARGE").otherwise("SMALL").alias("prev_budget_tier"),
        F.when(F.col("next_budget") >= 4181000, "PREMIUM").otherwise("STANDARD").alias("next_budget_tier"),
    )
    .agg(
        F.round(F.avg("next_budget"), 2).alias("avg_next_budget"),
        F.count(F.when(F.col("location") == "London", 1)).alias("london_rows"),
        F.avg(F.when(F.col("location") == "London", F.col("last7_min_budget"))).alias("london_avg_last7_min_budget"),
        F.avg("budget").alias("avg_budget"),
    )
)

result = (
    daily
    .select(
        "prev_budget_tier",
        "next_budget_tier",
        "avg_next_budget",
        "london_avg_last7_min_budget",
        "avg_budget",
    )
    .orderBy(F.col("london_avg_last7_min_budget").asc_nulls_last())
)
