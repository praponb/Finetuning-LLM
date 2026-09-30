from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_first())
w2 = Window.partitionBy("location")
w3 = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_first())
w4 = Window.orderBy(F.col("budget").desc_nulls_first())
w_part = Window.orderBy(F.col("budget").desc_nulls_last())
w6 = Window.partitionBy("location").orderBy(F.col("budget_dense_rank").desc_nulls_first())

valid = (
    df_departments
    .filter(F.col("budget").isNotNull())
    .select("*", F.initcap(F.trim("dept_name")).alias("dept_name_title"))
)

kept_keys = (
    valid
    .select("location", "id")
    .exceptAll(
        df_departments
        .filter(F.col("budget") < 1792000)
        .select("location", "id")
    )
)

verified = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "dept_name",
        "location",
        "budget",
        F.cume_dist().over(w).alias("budget_cume_dist"),
    )
)

cleaned = (
    metrics
    .filter((F.col("dept_name").eqNullSafe(F.lit("HR"))) & F.col("dept_name").isin(["Sales", "Finance", "Marketing"]))
    .select(
        "*",
        F.concat("location", F.lit(" "), "dept_name").alias("location_dept_name"),
    )
)

enriched = (
    cleaned
    .select(
        "location",
        "budget",
        "location_dept_name",
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share"),
        F.percent_rank().over(w3).alias("budget_percent_rank"),
        F.dense_rank().over(w4).alias("budget_dense_rank"),
        F.percent_rank().over(w_part).alias("budget_percent_rank_2"),
        F.avg("budget_cume_dist").over(w2).alias("location_avg_budget_cume_dist"),
    )
)

cleaned_departments = (
    enriched
    .filter(F.col("location").isNotNull() & F.col("budget_dense_rank").isNull())
    .select(
        "location",
        "budget",
        "location_dept_name",
        "budget_share",
        "budget_percent_rank",
        "budget_dense_rank",
        "budget_percent_rank_2",
        (F.col("budget_dense_rank") - F.col("budget_percent_rank")).alias("budget_dense_rank_minus_budget_percent_rank"),
        F.coalesce("location", F.lit("N/A")).alias("location_clean"),
    )
)

with_windows = (
    cleaned_departments
    .withColumn("location_sum_budget", F.sum("budget").over(w2))
    .withColumn("location_count_budget", F.count("budget").over(w2))
    .withColumn("budget_dense_rank_percent_rank", F.percent_rank().over(w6))
)

result = (
    with_windows
    .select(
        "budget",
        "budget_share",
        "budget_percent_rank_2",
        "location_clean",
        "location_sum_budget",
        "location_count_budget",
    )
)
