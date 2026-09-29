from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_last())
w2 = Window.partitionBy("dept_name")
w3 = Window.orderBy(F.col("budget_percent_rank").desc_nulls_first())
w4 = Window.partitionBy("location")
w_hist = Window.partitionBy("location", "dept_name").orderBy("budget_int")
w6 = Window.partitionBy("location", "dept_name").orderBy(F.col("budget_percent_rank_dense_rank").asc_nulls_last())
w_ordered = Window.partitionBy("location").orderBy(F.col("dept_name_max_budget_int").desc_nulls_first())
w_ordered_2 = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc_nulls_first())

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("budget") <= 1161000))
    .withColumn("budget_int", F.col("budget").cast("int"))
)

matched_keys = (
    filtered
    .select("location", "dept_name")
    .subtract(
        df_departments
        .filter(~F.col("dept_name").isin(["Finance", "Engineering"]))
        .select("location", "dept_name")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.full_name").like("%Carol%")), "left_semi")
)

with_windows = (
    eligible
    .withColumn("budget_percent_rank", F.percent_rank().over(w_running))
    .withColumn("dept_name_max_budget_int", F.max("budget_int").over(w2))
)

with_windows_departments = (
    with_windows
    .select(
        "*",
        F.dense_rank().over(w3).alias("budget_percent_rank_dense_rank"),
        F.avg("budget_percent_rank").over(w4).alias("location_avg_budget_percent_rank"),
    )
)

matched_keys_2 = (
    with_windows_departments
    .select("id")
    .subtract(
        df_departments
        .filter(F.col("budget") < 2125000)
        .select("id")
    )
)

kept_rows = (
    with_windows_departments
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .withColumn("budget_int_rank", F.rank().over(w_hist))
    .withColumn("dept_name_count_location_avg_budget_percent_rank", F.count("location_avg_budget_percent_rank").over(w2))
    .withColumn("budget_percent_rank_dense_rank_dense_rank", F.dense_rank().over(w6))
    .withColumn("dept_name_max_budget_int_rank", F.rank().over(w_ordered))
    .withColumn("budget_rank", F.rank().over(w_ordered_2))
)

stats = (
    scored
    .groupBy(
        F.when(F.col("budget_percent_rank_dense_rank") >= 6, "HIGH").otherwise("LOW").alias("budget_percent_rank_dense_rank_tier"),
    )
    .agg(
        F.percentile_approx("dept_name_max_budget_int", 0.95).alias("dept_name_max_budget_int_p95"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
    )
    .filter(F.col("num_dept_names") >= 1)
)

result = stats.select("budget_percent_rank_dense_rank_tier")
