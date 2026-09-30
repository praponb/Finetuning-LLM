from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id")
w2 = Window.partitionBy("manager_id").orderBy("salary")
w3 = Window.partitionBy("manager_id").orderBy(F.col("next_salary").desc(), "id")
w4 = Window.partitionBy("dept_id").orderBy("salary", "id")
w5 = Window.partitionBy("manager_id", "job_title").orderBy(F.col("next_salary_int").desc(), "id")
w6 = Window.orderBy("row_seq", "id")
w_grp = Window.partitionBy("job_title", "level").orderBy("next_salary_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("level").orderBy("salary", "id")
w9 = Window.partitionBy("job_title").orderBy("row_seq", "id")
w10 = Window.partitionBy("job_title").orderBy(F.col("seq_no_scaled").desc(), "id")

filtered = (
    df_employees
    .filter(F.col("salary") <= 103000)
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.col("salary").cast("int").alias("salary_int"),
        F.lower("job_title").alias("job_title_lower"),
        F.coalesce("level", F.lit("OTHER")).alias("resolved_level"),
    )
)

scored = (
    filtered
    .filter(F.col("is_remote"))
    .select(
        "*",
        F.lead("salary").over(w).alias("next_salary"),
        F.lag("salary").over(w).alias("prev_salary"),
        F.lead("salary_int").over(w).alias("next_salary_int"),
        F.lag("salary_int").over(w).alias("prev_salary_int"),
        F.row_number().over(w).alias("row_seq"),
        F.dense_rank().over(w2).alias("salary_dense_rank"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.ntile(5).over(w3).alias("next_salary_bucket"),
        F.lead("next_salary_int", 1, 0).over(w4).alias("next_next_salary_int"),
        F.row_number().over(w5).alias("seq_no"),
        F.ntile(5).over(w6).alias("row_seq_bucket"),
    )
)

base = (
    enriched
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-05-07"))) & (~F.col("level").eqNullSafe(F.lit("L3"))))
    .select(
        "id",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        "salary_int",
        "job_title_lower",
        "resolved_level",
        "next_salary",
        "prev_salary",
        "next_salary_int",
        "row_seq",
        "salary_dense_rank",
        "next_salary_bucket",
        "seq_no",
        "row_seq_bucket",
        F.round((F.col("seq_no") * 1.15), 2).alias("seq_no_scaled"),
        F.when(F.col("next_next_salary_int") >= 76, "High").when(F.col("next_next_salary_int") >= 29, "Medium").otherwise("Low").alias("next_next_salary_int_band"),
    )
)

metrics = (
    base
    .select(
        "id",
        "email",
        "job_title",
        "salary",
        "job_title_lower",
        "next_salary",
        "row_seq",
        "salary_dense_rank",
        "next_salary_bucket",
        "row_seq_bucket",
        "seq_no_scaled",
        F.max("next_salary").over(w_grp).alias("running_max_next_salary"),
        F.min("next_salary").over(w_grp).alias("running_min_next_salary"),
        F.sum("next_salary").over(w_grp).alias("running_sum_next_salary"),
        F.lead("seq_no", 2, 0).over(w8).alias("next_seq_no"),
    )
)

ranked = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w9))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    ranked
    .select(
        "id",
        "job_title_lower",
        "next_salary",
        "row_seq",
        "salary_dense_rank",
        "seq_no_scaled",
        "running_max_next_salary",
        "running_min_next_salary",
        "dedup_rank",
        F.ntile(10).over(w10).alias("seq_no_scaled_bucket"),
        F.lag("row_seq_bucket", 2, 0).over(w9).alias("prev_row_seq_bucket"),
    )
)

result = (
    with_windows
    .withColumn("running_min_next_salary_tier", F.when(F.col("running_min_next_salary") >= 137000, "PREMIUM").otherwise("STANDARD"))
    .withColumn("seq_no_scaled_tier", F.when(F.col("seq_no_scaled") >= 68, "LARGE").otherwise("SMALL"))
    .cube("running_min_next_salary_tier", "seq_no_scaled_tier")
    .agg(
        F.percentile_approx("row_seq", 0.5).alias("row_seq_p50"),
        F.round(F.avg("running_min_next_salary"), 2).alias("avg_running_min_next_salary"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("running_max_next_salary"), 2).alias("avg_running_max_next_salary"),
        F.grouping("running_min_next_salary_tier").alias("grouping_level"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("distinct_ids").desc())
)
