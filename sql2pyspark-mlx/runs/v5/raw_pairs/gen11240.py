from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("location", "dept_name").orderBy("budget_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location", "dept_name").orderBy("budget")

valid = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("location") == "Berlin"))
    .withColumn("dept_name_upper", F.upper("dept_name"))
    .withColumn("budget_rounded", F.round("budget", 2))
)

with_windows = (
    valid
    .filter(F.col("location") != "Singapore")
    .select(
        "id",
        "dept_name",
        "dept_name_upper",
        "budget_rounded",
        F.avg("budget_rounded").over(w_part).alias("running_avg_budget_rounded"),
        F.count("budget_rounded").over(w_part).alias("running_count_budget_rounded"),
        F.sum("budget_rounded").over(w_part).alias("running_sum_budget_rounded"),
        F.max("budget_rounded").over(w_part).alias("running_max_budget_rounded"),
        F.percent_rank().over(w2).alias("budget_percent_rank"),
    )
)

kept_keys = (
    with_windows
    .select("dept_name", "id")
    .intersectAll(
        df_departments
        .filter(F.col("location") != "New York")
        .select("dept_name", "id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.level").isin(["L4", "L5"])), "left_anti")
)

valid_departments = (
    eligible
    .filter((F.col("dept_name").eqNullSafe(F.lit("Marketing"))) & F.col("dept_name").isNotNull())
    .select(
        "dept_name",
        "dept_name_upper",
        "budget_rounded",
        "running_avg_budget_rounded",
        "running_sum_budget_rounded",
        "running_max_budget_rounded",
        "budget_percent_rank",
        F.coalesce("budget_percent_rank", F.lit(0)).alias("budget_percent_rank_filled"),
        F.coalesce("dept_name", F.lit("NONE")).alias("dept_name_filled"),
    )
)

result = (
    valid_departments
    .select(
        "dept_name",
        "dept_name_upper",
        "running_avg_budget_rounded",
        "running_max_budget_rounded",
        "budget_percent_rank",
        "budget_percent_rank_filled",
        "dept_name_filled",
    )
    .orderBy(F.col("running_max_budget_rounded").desc())
)
