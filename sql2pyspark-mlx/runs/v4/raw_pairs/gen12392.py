from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_first())
w2 = Window.partitionBy("location", "dept_name")
w3 = Window.orderBy(F.col("budget").asc_nulls_first())
w4 = Window.partitionBy("location")
w5 = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc())

kept_keys = (
    df_departments
    .select("location", "id")
    .subtract(
        df_departments
        .filter(~F.col("dept_name").isin(["Finance", "Marketing", "Legal"]))
        .select("location", "id")
    )
)

verified = (
    df_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .select(
        "*",
        F.dense_rank().over(w).alias("budget_dense_rank"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share"),
        F.rank().over(w3).alias("budget_rank"),
        F.max("budget").over(w4).alias("location_max_budget"),
        F.dense_rank().over(w5).alias("budget_dense_rank_2"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("location").isNotNull() & (F.col("budget_rank") <= 6))
    .select(
        "id",
        "dept_name",
        "budget",
        "budget_dense_rank",
        "budget_share",
        "budget_rank",
        F.least("location_max_budget", "budget_rank").alias("least_location_max_budget_budget_rank"),
        F.coalesce("location", F.lit("NONE")).alias("location_filled"),
    )
)

base = (
    cleaned
    .filter(F.col("dept_name").isNotNull() & (F.col("least_location_max_budget_budget_rank") >= 43))
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
)

grouped = (
    base
    .groupBy(
        F.when(F.col("budget_filled") >= 70, "LARGE").otherwise("SMALL").alias("budget_filled_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_count"),
        F.round(F.sum("budget_filled") / F.nullif(F.count(F.when(F.col("dept_name") == "Engineering", 1)), F.lit(0)), 2).alias("budget_filled_per_engineering"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list_2"),
    )
)

result = grouped.select("budget_filled_tier", "marketing_count", "dept_name_list_2")
