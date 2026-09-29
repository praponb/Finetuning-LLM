from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("dept_name") == "Sales"))
    .select(
        "*",
        F.when(F.col("dept_name").isin(["HR", "Marketing"]), "Group A").otherwise("Other").alias("dept_name_group"),
    )
)

metrics = (
    base
    .select(
        "id",
        "dept_name_group",
        F.count("budget").over(w).alias("last3_count_budget"),
        F.avg("budget").over(w).alias("last3_avg_budget"),
        F.sum("budget").over(w).alias("last3_sum_budget"),
    )
)

filtered = (
    metrics
    .filter(F.col("last3_count_budget") == 13)
    .select(
        "*",
        F.coalesce("last3_sum_budget", F.lit(0)).alias("last3_sum_budget_filled"),
    )
)

base_departments = (
    filtered
    .filter(F.col("last3_count_budget") <= 15)
    .select(
        "id",
        "last3_count_budget",
        "last3_sum_budget",
        "last3_sum_budget_filled",
        F.round("last3_sum_budget_filled", 1).alias("last3_sum_budget_filled_rounded"),
    )
)

result = (
    base_departments
    .select("id", "last3_sum_budget", "last3_sum_budget_filled")
    .orderBy(F.col("last3_sum_budget_filled").asc_nulls_first())
)
