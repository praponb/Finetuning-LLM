from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").asc_nulls_last())
w2 = Window.partitionBy("dept_name")
w3 = Window.partitionBy("location")
w4 = Window.orderBy(F.col("budget").desc())
w_grp = Window.orderBy(F.col("budget_rank").asc_nulls_last())

kept_keys = (
    df_departments
    .select("location")
    .subtract(
        df_departments
        .filter(F.col("location").isin(["Tokyo", "Berlin", "Chiang Mai"]))
        .select("location")
    )
)

kept_rows = (
    df_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .select(
        "*",
        F.rank().over(w).alias("budget_rank"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share"),
        F.round((F.col("budget") / F.sum("budget").over(w3)), 4).alias("budget_share_2"),
        F.max("budget").over(w2).alias("dept_name_max_budget"),
    )
)

base = (
    with_windows
    .filter(F.col("location").isNotNull())
    .select(
        "id",
        "budget",
        "budget_rank",
        "budget_share",
        "budget_share_2",
        "dept_name_max_budget",
        F.col("budget").cast("int").alias("budget_int"),
        F.substring("location", 1, 3).alias("location_prefix"),
    )
)

with_windows_departments = (
    base
    .select(
        "*",
        F.percent_rank().over(w4).alias("budget_percent_rank"),
        F.percent_rank().over(w_grp).alias("budget_rank_percent_rank"),
    )
)

result = (
    with_windows_departments
    .cube(
        F.when(F.col("budget_share_2") >= 0.61, "PREMIUM").otherwise("STANDARD").alias("budget_share_2_tier"),
    )
    .agg(
        F.percentile_approx("budget_share_2", [0.25, 0.5, 0.75]).alias("budget_share_2_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.percentile_approx("dept_name_max_budget", 0.5).alias("dept_name_max_budget_p50"),
    )
    .orderBy(F.col("budget_share_2_tier").desc_nulls_last())
)
