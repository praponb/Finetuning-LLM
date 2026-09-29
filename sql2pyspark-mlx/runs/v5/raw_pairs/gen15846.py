from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w_grp = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").asc_nulls_last())

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("location_lower", F.lower("location"))
    .withColumn("resolved_location", F.coalesce("location", F.lit("DIRECT")))
)

scored = (
    base
    .select(
        "id",
        "dept_name",
        "budget",
        "location_lower",
        F.ntile(10).over(w).alias("budget_bucket"),
        F.dense_rank().over(w_grp).alias("budget_dense_rank"),
    )
)

cleaned = (
    scored
    .filter(~F.col("dept_name").eqNullSafe(F.lit("Marketing")))
    .select(
        "id",
        "dept_name",
        "budget",
        "location_lower",
        "budget_bucket",
        "budget_dense_rank",
        F.coalesce("dept_name", F.lit("unassigned")).alias("dept_name_filled"),
    )
)

filtered = (
    cleaned
    .filter(F.col("budget_dense_rank") == 6)
    .select(
        "*",
        F.ceil("budget_bucket").alias("budget_bucket_whole"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("dept_name_filled_2"),
    )
)

remaining_keys = (
    filtered
    .select("id", "dept_name")
    .intersectAll(
        df_departments
        .filter(~F.col("dept_name").isin(["Sales", "Engineering"]))
        .select("id", "dept_name")
    )
)

verified = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "dept_name",
        "budget",
        "location_lower",
        "budget_bucket",
        "budget_bucket_whole",
        "dept_name_filled_2",
    )
    .orderBy(F.col("budget_bucket").asc_nulls_last(), "budget_bucket_whole")
)
