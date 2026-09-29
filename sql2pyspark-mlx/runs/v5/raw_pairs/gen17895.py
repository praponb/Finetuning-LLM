from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_first(), "id")
w_hist = Window.partitionBy("dept_name").orderBy("latest_rank", "id").rowsBetween(-3, Window.currentRow)
w_hist_2 = Window.partitionBy("location").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("location").orderBy(F.col("last4_count_budget").desc_nulls_last(), "id")

filtered = (
    df_departments
    .filter((~F.col("dept_name").eqNullSafe(F.lit("HR"))) & (F.col("location") == "Chiang Mai"))
    .withColumn("dept_name_prefix", F.substring("dept_name", 1, 4))
)

latest = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    latest
    .filter(F.col("budget") <= 2162000)
    .select(
        "id",
        "location",
        "latest_rank",
        F.count("budget").over(w_hist).alias("last4_count_budget"),
        F.min("budget").over(w_hist).alias("last4_min_budget"),
        F.avg("budget").over(w_hist).alias("last4_avg_budget"),
        F.sum("budget").over(w_hist_2).alias("cumulative_budget"),
    )
)

deduped = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

kept = (
    deduped
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.level").isin(["L5", "L1"])), "left_semi")
)

result = (
    kept
    .select(
        "id",
        "latest_rank",
        "last4_min_budget",
        "last4_avg_budget",
        "cumulative_budget",
    )
)
