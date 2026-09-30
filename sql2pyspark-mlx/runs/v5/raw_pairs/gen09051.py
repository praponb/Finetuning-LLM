from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc_nulls_first())
w2 = Window.partitionBy("dept_name").orderBy("budget", "id")
w3 = Window.orderBy("budget_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_name").orderBy(F.col("budget_scaled").desc(), "id")
w5 = Window.partitionBy("dept_name").orderBy("cumulative_budget_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("dept_name").orderBy(F.col("budget_dense_rank").desc(), "id")
w7 = Window.partitionBy("dept_name").orderBy("next_budget", "id").rowsBetween(-6, -1)
w8 = Window.partitionBy("dept_name").orderBy("next_budget_times_seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("budget_scaled", F.round((F.col("budget") * 0.85), 2))
)

enriched = (
    valid
    .filter(F.col("budget") > 1673000)
    .select(
        "id",
        "dept_name",
        "budget_scaled",
        F.dense_rank().over(w).alias("budget_dense_rank"),
        F.lead("budget", 1, 0).over(w2).alias("next_budget"),
        F.first("budget", ignorenulls=True).over(w3).alias("first_known_budget"),
        F.sum("budget_scaled").over(w3).alias("cumulative_budget_scaled"),
    )
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    deduped
    .select(
        "*",
        F.first("first_known_budget", ignorenulls=True).over(w5).alias("first_known_first_known_budget"),
        F.ntile(3).over(w4).alias("budget_scaled_bucket"),
        F.row_number().over(w6).alias("seq_no"),
        F.avg("first_known_budget").over(w7).alias("first_known_budget_avg_6_prior"),
    )
)

base = (
    with_windows
    .filter(F.col("dept_name").isNotNull() & (F.col("first_known_budget_avg_6_prior") == 3894526))
    .select(
        "*",
        (F.col("next_budget") * F.col("seq_no")).alias("next_budget_times_seq_no"),
        F.coalesce("dept_name", F.lit("N/A")).alias("dept_name_filled"),
    )
)

enriched_departments = (
    base
    .select(
        "id",
        "budget_scaled",
        "budget_dense_rank",
        "next_budget",
        "first_known_budget",
        "first_known_first_known_budget",
        "budget_scaled_bucket",
        "seq_no",
        "first_known_budget_avg_6_prior",
        F.max("cumulative_budget_scaled").over(w8).alias("running_max_cumulative_budget_scaled"),
        F.min("cumulative_budget_scaled").over(w8).alias("running_min_cumulative_budget_scaled"),
        F.sum("cumulative_budget_scaled").over(w8).alias("running_sum_cumulative_budget_scaled"),
        F.count("cumulative_budget_scaled").over(w8).alias("running_count_cumulative_budget_scaled"),
    )
)

result = (
    enriched_departments
    .select(
        "id",
        "next_budget",
        "first_known_budget",
        "budget_scaled_bucket",
        "seq_no",
        "first_known_budget_avg_6_prior",
        "running_max_cumulative_budget_scaled",
        "running_min_cumulative_budget_scaled",
    )
    .orderBy(F.col("first_known_budget_avg_6_prior").desc_nulls_last())
)
