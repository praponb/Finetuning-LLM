from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("is_l3").desc(), "id")
w_ordered = Window.orderBy("is_l3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_id").orderBy("is_l3_bucket", "id")
w4 = Window.partitionBy("dept_id", "manager_id").orderBy(F.col("is_l3_bucket").desc_nulls_last(), "id")
w5 = Window.orderBy("is_l3_bucket", "id")
w6 = Window.partitionBy("level").orderBy("is_l3_bucket")
w7 = Window.partitionBy("job_title").orderBy(F.col("is_l3_bucket").desc())
w8 = Window.orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("manager_id")
w_hist_2 = Window.partitionBy("manager_id", "dept_id").orderBy("is_l3_bucket_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-08-16").cast("date")) & F.col("job_title").isNotNull())
    .withColumn("job_title_level_key", F.concat_ws(",", "job_title", "level"))
    .withColumn("is_l3", F.when(F.col("level") == "L3", 1).otherwise(0))
)

enriched = (
    cleaned
    .filter(F.col("manager_id").isin([25, 26, 29]))
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "level",
        "job_title_level_key",
        F.ntile(4).over(w).alias("is_l3_bucket"),
        F.first("job_title", ignorenulls=True).over(w_ordered).alias("first_known_job_title"),
    )
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

deduped = (
    latest
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

metrics = (
    deduped
    .select(
        "*",
        F.ntile(10).over(w5).alias("is_l3_bucket_bucket"),
        F.percent_rank().over(w6).alias("is_l3_bucket_percent_rank"),
    )
)

with_windows = (
    metrics
    .withColumn("is_l3_bucket_dense_rank", F.dense_rank().over(w7))
    .withColumn("last_known_job_title", F.last("job_title", ignorenulls=True).over(w8))
    .withColumn("manager_id_sum_is_l3_bucket_percent_rank", F.sum("is_l3_bucket_percent_rank").over(w_hist))
    .withColumn("cumulative_is_l3_bucket_percent_rank", F.sum("is_l3_bucket_percent_rank").over(w_hist_2))
    .withColumn("is_l3_bucket_bucket_2", F.ntile(10).over(w3))
)

valid = (
    with_windows
    .filter(F.col("level").isNotNull() & (F.col("is_l3_bucket_bucket") <= 5))
    .select(
        "id",
        "email",
        "manager_id",
        "level",
        "job_title_level_key",
        "is_l3_bucket",
        "first_known_job_title",
        "is_l3_bucket_bucket",
        "is_l3_bucket_percent_rank",
        "manager_id_sum_is_l3_bucket_percent_rank",
        "cumulative_is_l3_bucket_percent_rank",
        F.when(F.col("job_title") == "Analyst", 1).otherwise(0).alias("is_analyst"),
        F.lower("email").alias("email_lower"),
    )
)

result = (
    valid
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["employee_level", "job_title"], ["employee_level"], []],
        F.col("p.level").alias("employee_level"),
        F.col("p.job_title").alias("job_title"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("e.is_l3_bucket")).alias("min_is_l3_bucket"),
        F.round(F.avg(F.col("e.is_l3_bucket_bucket")), 2).alias("avg_is_l3_bucket_bucket"),
    )
)
