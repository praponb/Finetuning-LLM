from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name")
w2 = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").asc_nulls_first())
w3 = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_last())

remaining_keys = (
    df_departments
    .select("dept_name", "location")
    .intersectAll(
        df_departments
        .filter(F.col("location") != "New York")
        .select("dept_name", "location")
    )
)

verified = (
    df_departments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("location") == "Tokyo")
    .select(
        "*",
        F.when(F.col("budget") >= 3565000, "High").when(F.col("budget") >= 1562000, "Medium").otherwise("Low").alias("budget_level"),
        F.substring("location", 1, 4).alias("location_prefix"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("dept_name_filled"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.sum("budget").over(w).alias("location_sum_budget"),
        F.rank().over(w2).alias("budget_rank"),
        F.dense_rank().over(w3).alias("budget_dense_rank"),
    )
)

result = (
    enriched
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "location_prefix",
        "dept_name_filled",
        "budget_dense_rank",
    )
)
