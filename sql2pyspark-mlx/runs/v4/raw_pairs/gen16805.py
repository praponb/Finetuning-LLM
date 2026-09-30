from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")
w2 = Window.partitionBy("dept_name", "location")
w_recent = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_first())
w4 = Window.orderBy(F.col("dept_name_min_budget_rounded").desc())

valid = (
    df_departments
    .filter(F.col("location") == "Bangkok")
    .select(
        "*",
        F.round("budget", 2).alias("budget_rounded"),
        F.concat_ws(",", "dept_name", "location").alias("dept_name_location_key"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("resolved_dept_name"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.dept_id").isin([5, 6, 16, 22])), "left_anti")
)

filtered = (
    kept
    .filter(F.col("location").isNotNull())
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "budget_rounded",
        "dept_name_location_key",
        "resolved_dept_name",
        F.lower("dept_name").alias("dept_name_lower"),
        F.coalesce("dept_name", F.lit("DIRECT")).alias("dept_name_clean"),
    )
)

remaining_keys = (
    filtered
    .select("id", "dept_name")
    .subtract(
        df_departments
        .filter(F.col("dept_name") != "Sales")
        .select("id", "dept_name")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "*",
        F.min("budget_rounded").over(w).alias("dept_name_min_budget_rounded"),
        F.min("budget").over(w2).alias("dept_name_min_budget"),
        F.rank().over(w_recent).alias("budget_rank"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.rank().over(w4).alias("dept_name_min_budget_rounded_rank"),
        F.count("budget_rank").over(w).alias("dept_name_count_budget_rank"),
    )
)

matched_keys = (
    enriched
    .select("dept_name", "location")
    .exceptAll(
        df_departments
        .filter(F.col("dept_name").isNotNull())
        .select("dept_name", "location")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("dept_name_min_budget_rounded_rank") >= 7, "LARGE").otherwise("SMALL").alias("dept_name_min_budget_rounded_rank_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list_2"),
    )
)
