from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")
w2 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("budget", "id")
w4 = Window.orderBy("dept_name_max_budget", "id")

filtered = (
    df_departments
    .filter(F.col("dept_name") == "Marketing")
    .select(
        "*",
        F.when(F.col("budget") >= 3097000, "High").when(F.col("budget") >= 1883000, "Medium").otherwise("Low").alias("budget_tier"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "budget",
        "budget_tier",
        F.max("budget").over(w).alias("dept_name_max_budget"),
        F.sum("budget").over(w2).alias("cumulative_budget"),
    )
)

with_windows = (
    enriched
    .filter(F.col("dept_name_max_budget") < 2451854)
    .select(
        "id",
        "budget",
        "budget_tier",
        "dept_name_max_budget",
        "cumulative_budget",
        F.lead("budget").over(w3).alias("next_budget"),
        F.lag("cumulative_budget").over(w3).alias("prev_cumulative_budget"),
        F.lead("cumulative_budget").over(w3).alias("next_cumulative_budget"),
        F.row_number().over(w3).alias("seq_no"),
        F.lead("budget", 1).over(w4).alias("next_budget_2"),
    )
)

matched_keys = (
    with_windows
    .select("id")
    .subtract(
        df_departments
        .filter(F.col("budget") <= 2724000)
        .select("id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    verified
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (~F.col("c.level").isin(["L1", "L3"])), "left_anti")
)

result = (
    kept
    .withColumn("next_budget_2_tier", F.when(F.col("next_budget_2") >= 3906000, "PREMIUM").otherwise("STANDARD"))
    .withColumn("seq_no_tier", F.when(F.col("seq_no") >= 8, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["next_budget_2_tier", "seq_no_tier"], ["next_budget_2_tier"], []],
        "next_budget_2_tier",
        "seq_no_tier",
    )
    .agg(
        F.percentile_approx("next_budget_2", 0.5).alias("next_budget_2_p50"),
        F.percentile_approx("next_cumulative_budget", 0.9).alias("next_cumulative_budget_p90"),
        F.round(F.avg("prev_cumulative_budget"), 2).alias("avg_prev_cumulative_budget"),
        F.grouping("next_budget_2_tier").alias("grouping_level"),
    )
)
