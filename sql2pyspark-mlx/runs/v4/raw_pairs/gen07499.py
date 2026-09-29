from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary", "id")
w_running = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("dept_id", "job_title").orderBy(F.col("running_salary").desc())
w6 = Window.partitionBy("manager_id", "level").orderBy(F.col("running_salary_rank").desc(), "id")
w7 = Window.partitionBy("dept_id", "level").orderBy(F.col("running_salary_rank").desc_nulls_first())
w8 = Window.partitionBy("level", "dept_id")
w9 = Window.partitionBy("manager_id", "level").orderBy("hire_date", "id")
w10 = Window.partitionBy("job_title").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("dept_id").orderBy("running_salary_rank", "id")

enriched = (
    df_employees
    .select(
        "*",
        F.ntile(5).over(w).alias("salary_bucket"),
        F.sum("salary").over(w_running).alias("running_salary"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.first("salary_bucket", ignorenulls=True).over(w_hist).alias("first_known_salary_bucket"),
        F.first("level", ignorenulls=True).over(w4).alias("first_known_level"),
        F.rank().over(w_recent).alias("running_salary_rank"),
    )
)

deduped = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    deduped
    .select(
        "*",
        F.cume_dist().over(w7).alias("running_salary_rank_cume_dist"),
        F.stddev_samp("first_known_salary_bucket").over(w8).alias("level_stddev_samp_first_known_salary_bucket"),
        F.row_number().over(w9).alias("seq_no"),
        F.last("salary_bucket", ignorenulls=True).over(w10).alias("last_known_salary_bucket"),
        F.row_number().over(w11).alias("row_seq"),
    )
)

result = (
    metrics
    .groupBy(F.date_trunc("QUARTER", "hire_date").alias("quarter_start"))
    .agg(
        F.percentile_approx("running_salary_rank", 0.5).alias("running_salary_rank_p50"),
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_rows"),
        F.percentile_approx("running_salary", [0.25, 0.5, 0.75]).alias("running_salary_quartiles"),
    )
    .orderBy(F.col("quarter_start").desc())
)
