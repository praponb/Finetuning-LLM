from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location").orderBy("budget", "id")
w3 = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").asc_nulls_first())
w4 = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").asc_nulls_last())
w5 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w6 = Window.orderBy(F.col("prev_budget").desc_nulls_last())
w_recent = Window.orderBy("cumulative_budget", "id")
w8 = Window.orderBy("row_seq", "id")
w9 = Window.orderBy(F.col("prev_budget").desc(), "id")
w10 = Window.partitionBy("row_seq_2_tier").orderBy(F.col("cnt").desc())

filtered = (
    df_departments
    .filter(F.col("budget") > 2665000)
    .select(
        "*",
        F.initcap(F.trim("location")).alias("location_title"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("dept_name_filled"),
    )
)

enriched = (
    filtered
    .withColumn("cumulative_budget", F.sum("budget").over(w))
    .withColumn("prev_budget", F.lag("budget", 1, 0).over(w2))
    .withColumn("budget_rank", F.rank().over(w3))
    .withColumn("budget_rank_2", F.rank().over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
)

eligible = (
    enriched
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.lower(F.col("ch.full_name")).like("%ack%")), "left_anti")
)

filtered_departments = (
    eligible
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.col("cumulative_budget").cast("string").alias("cumulative_budget_num"),
    )
)

filtered_departments_2 = (
    filtered_departments
    .filter(F.col("dept_name").isNotNull())
    .select(
        "id",
        "dept_name_filled",
        "cumulative_budget",
        "prev_budget",
        "row_seq",
        "cumulative_budget_num",
        F.round("budget_rank", 2).alias("budget_rank_rounded"),
        F.coalesce("dept_name", F.lit("DIRECT")).alias("dept_name_clean"),
    )
)

enriched_departments = (
    filtered_departments_2
    .withColumn("prev_budget_dense_rank", F.dense_rank().over(w6))
    .withColumn("row_seq_2", F.row_number().over(w_recent))
    .withColumn("row_seq_bucket", F.ntile(10).over(w8))
    .withColumn("prev_budget_bucket", F.ntile(4).over(w9))
)

grouped = (
    enriched_departments
    .groupBy(
        F.when(F.col("row_seq_2") >= 7, "PREMIUM").otherwise("STANDARD").alias("row_seq_2_tier"),
        F.when(F.col("prev_budget_dense_rank") >= 7, "HIGH").otherwise("LOW").alias("prev_budget_dense_rank_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("cnt_2"),
        F.percentile_approx("prev_budget_bucket", [0.25, 0.5, 0.75]).alias("prev_budget_bucket_quartiles"),
        F.max("prev_budget_dense_rank").alias("max_prev_budget_dense_rank"),
        F.percentile_approx("row_seq_bucket", [0.25, 0.5, 0.75]).alias("row_seq_bucket_quartiles"),
        F.percentile_approx("cumulative_budget", 0.95).alias("cumulative_budget_p95"),
    )
    .withColumn("cnt_percent_rank", F.percent_rank().over(w10))
)

result = (
    grouped
    .select(
        "row_seq_2_tier",
        "prev_budget_dense_rank_tier",
        "cnt",
        "cnt_2",
        "max_prev_budget_dense_rank",
        "row_seq_bucket_quartiles",
        "cumulative_budget_p95",
        "cnt_percent_rank",
    )
    .orderBy("cumulative_budget_p95")
)
