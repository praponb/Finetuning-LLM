from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")
w_recent = Window.orderBy("budget", "id")
w3 = Window.partitionBy("dept_name").orderBy("dept_name_min_budget")
w_ordered = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_last())
w5 = Window.partitionBy("location").orderBy("budget", "id")
w6 = Window.partitionBy("location", "dept_name")
w7 = Window.orderBy(F.col("budget_dense_rank").desc_nulls_first())
w8 = Window.partitionBy("dept_name").orderBy("budget_dense_rank")
w9 = Window.orderBy(F.col("dept_name_min_budget").asc_nulls_last())
w10 = Window.partitionBy("dept_name", "location")

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & ~F.col("location").isin(["London", "New York", "Bangkok"]))
    .select(
        "*",
        F.when(F.col("budget") >= 4472000, "High").when(F.col("budget") >= 1427000, "Medium").otherwise("Low").alias("budget_tier"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.month(F.col("c.hire_date")).isin([1, 10, 11])), "left_semi")
)

metrics = (
    kept
    .select(
        "*",
        F.min("budget").over(w).alias("dept_name_min_budget"),
        F.ntile(5).over(w_recent).alias("budget_bucket"),
        F.lag("budget", 1).over(w_recent).alias("prev_budget"),
    )
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    ranked
    .withColumn("budget_dense_rank", F.dense_rank().over(w_ordered))
    .withColumn("event_seq", F.row_number().over(w5))
    .withColumn("prev_budget_share", F.round((F.col("prev_budget") / F.sum("prev_budget").over(w)), 4))
)

remaining_keys = (
    enriched
    .select("id", "location")
    .intersectAll(
        df_departments
        .filter(F.col("budget") <= 1611000)
        .select("id", "location")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

metrics_departments = (
    kept_rows
    .select(
        "*",
        F.stddev_samp("budget_bucket").over(w6).alias("location_stddev_samp_budget_bucket"),
        F.cume_dist().over(w7).alias("budget_dense_rank_cume_dist"),
        F.rank().over(w8).alias("budget_dense_rank_rank"),
        F.dense_rank().over(w9).alias("dept_name_min_budget_dense_rank"),
        F.round((F.col("event_seq") / F.sum("event_seq").over(w10)), 4).alias("event_seq_share"),
    )
)

result = (
    metrics_departments
    .groupBy(
        "location",
        F.when(F.col("location_stddev_samp_budget_bucket") >= 8, "HIGH").otherwise("LOW").alias("location_stddev_samp_budget_bucket_tier"),
    )
    .agg(
        F.count_if(F.col("location") == "Chiang Mai").alias("chiang_mai_events"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_rows"),
    )
)
