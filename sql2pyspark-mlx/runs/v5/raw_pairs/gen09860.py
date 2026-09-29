from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")
w2 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_last())
w_recent = Window.partitionBy("dept_name").orderBy(F.col("budget_share").desc_nulls_first())
w_part = Window.partitionBy("dept_name")
w5 = Window.orderBy(F.col("budget").asc_nulls_first())
w_grp = Window.orderBy(F.col("budget").asc_nulls_last())
w7 = Window.partitionBy("location").orderBy(F.col("max_dept_name_max_budget_cume_dist").desc())

remaining_keys = (
    df_departments
    .select("dept_name", "id")
    .intersectAll(
        df_departments
        .filter(F.col("location").isNotNull())
        .select("dept_name", "id")
    )
)

verified = (
    df_departments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w)), 4))
    .withColumn("budget_cume_dist", F.cume_dist().over(w2))
)

eligible = (
    enriched
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (~F.col("c.is_remote")), "left_anti")
)

metrics = (
    eligible
    .filter(F.col("budget_cume_dist") <= 0.27)
    .withColumn("budget_share_cume_dist", F.cume_dist().over(w_recent))
    .withColumn("location_stddev_samp_budget_share", F.stddev_samp("budget_share").over(w))
    .withColumn("dept_name_max_budget_cume_dist", F.max("budget_cume_dist").over(w_part))
    .withColumn("budget_cume_dist_2", F.cume_dist().over(w5))
    .withColumn("budget_percent_rank", F.percent_rank().over(w_grp))
)

result = (
    metrics
    .groupBy(
        "location",
        F.when(F.col("budget_share") >= 0.63, "PREMIUM").otherwise("STANDARD").alias("budget_share_tier"),
    )
    .agg(
        F.max("dept_name_max_budget_cume_dist").alias("max_dept_name_max_budget_cume_dist"),
        F.percentile_approx("budget_share_cume_dist", [0.25, 0.5, 0.75]).alias("budget_share_cume_dist_quartiles"),
        F.count(F.when(F.col("location") == "Singapore", 1)).alias("singapore_rows"),
    )
    .withColumn("max_dept_name_max_budget_cume_dist_rank", F.rank().over(w7))
)
