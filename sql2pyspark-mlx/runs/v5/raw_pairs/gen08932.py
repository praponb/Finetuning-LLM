from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name")
w2 = Window.partitionBy("dept_name")
w3 = Window.partitionBy("location")
w4 = Window.partitionBy("location").orderBy(F.col("budget_share_2").desc())
w5 = Window.partitionBy("location").orderBy(F.col("location_avg_budget_share_3").asc_nulls_last())
w6 = Window.partitionBy("location").orderBy(F.col("budget_share").desc())
w7 = Window.orderBy(F.col("location_avg_budget_share_3").asc_nulls_first())
w8 = Window.orderBy("location_avg_budget_share_3")
w9 = Window.orderBy("budget_share")
w10 = Window.orderBy(F.col("location_avg_budget_share_3_percent_rank").asc_nulls_last())

remaining_keys = (
    df_departments
    .select("location", "dept_name")
    .subtract(
        df_departments
        .filter(F.col("dept_name").isin(["Finance", "Marketing"]))
        .select("location", "dept_name")
    )
)

verified = (
    df_departments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "id",
        "location",
        "budget",
        F.round((F.col("budget") / F.sum("budget").over(w)), 4).alias("budget_share"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share_2"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share_3"),
    )
)

with_windows = metrics.withColumn("location_avg_budget_share_3", F.avg("budget_share_3").over(w3))

scored = (
    with_windows
    .filter(F.col("budget") < 2992000)
    .select(
        "id",
        "location",
        "budget_share",
        "budget_share_3",
        "location_avg_budget_share_3",
        F.percent_rank().over(w4).alias("budget_share_2_percent_rank"),
        F.percent_rank().over(w5).alias("location_avg_budget_share_3_percent_rank"),
        F.percent_rank().over(w5).alias("location_avg_budget_share_3_percent_rank_2"),
        F.rank().over(w6).alias("budget_share_rank"),
    )
)

scored_departments = (
    scored
    .select(
        "budget_share",
        "location_avg_budget_share_3",
        "location_avg_budget_share_3_percent_rank",
        F.stddev_samp("budget_share_2_percent_rank").over(w3).alias("location_stddev_samp_budget_share_2_percent_rank"),
    )
)

enriched = (
    scored_departments
    .select(
        "*",
        F.dense_rank().over(w7).alias("location_avg_budget_share_3_dense_rank"),
        F.dense_rank().over(w8).alias("location_avg_budget_share_3_dense_rank_2"),
    )
)

result = (
    enriched
    .select(
        "location_avg_budget_share_3",
        "location_stddev_samp_budget_share_2_percent_rank",
        "location_avg_budget_share_3_dense_rank",
        "location_avg_budget_share_3_dense_rank_2",
        F.rank().over(w9).alias("budget_share_rank"),
        F.cume_dist().over(w10).alias("location_avg_budget_share_3_percent_rank_cume_dist"),
        F.dense_rank().over(w7).alias("location_avg_budget_share_3_dense_rank_3"),
    )
)
