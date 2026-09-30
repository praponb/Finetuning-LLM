from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("dept_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("job_title", "dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("row_seq").desc())
w4 = Window.partitionBy("job_title")
w5 = Window.orderBy(F.col("running_salary").desc_nulls_last())
w6 = Window.orderBy(F.col("salary").desc())
w7 = Window.orderBy(F.col("running_salary").asc_nulls_last())

with_windows = (
    df_employees
    .select(
        "id",
        "full_name",
        "email",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        F.lead("salary").over(w_part).alias("next_salary"),
        F.lag("salary").over(w_part).alias("prev_salary"),
        F.row_number().over(w_part).alias("row_seq"),
        F.sum("salary").over(w2).alias("running_salary"),
    )
)

matched_keys = (
    with_windows
    .select("id", "job_title")
    .intersectAll(
        df_employees
        .filter(F.col("full_name").like("%Hen%"))
        .select("id", "job_title")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter((F.col("hire_date") >= F.lit("2023-02-13").cast("date")) & (F.col("row_seq") > 5))
    .select("*", F.col("row_seq").cast("double").alias("row_seq_str"))
)

metrics = (
    base
    .withColumn("row_seq_rank", F.rank().over(w3))
    .withColumn("job_title_avg_row_seq", F.avg("row_seq").over(w4))
    .withColumn("running_salary_cume_dist", F.cume_dist().over(w5))
)

enriched = (
    metrics
    .select(
        "id",
        "full_name",
        "email",
        "prev_salary",
        F.dense_rank().over(w6).alias("salary_dense_rank"),
        F.round((F.col("row_seq") / F.sum("row_seq").over(w4)), 4).alias("row_seq_share"),
        F.rank().over(w7).alias("running_salary_rank"),
    )
)

result = (
    enriched
    .select("full_name", "email", "row_seq_share", "running_salary_rank")
    .orderBy("row_seq_share", "running_salary_rank")
)
