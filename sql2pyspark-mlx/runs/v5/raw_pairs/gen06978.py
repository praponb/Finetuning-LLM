from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level", "job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("job_title", "level").orderBy("hire_date", "id")
w_hist = Window.partitionBy("job_title").orderBy(F.col("seq_no").desc())
w5 = Window.partitionBy("job_title").orderBy(F.col("cumulative_salary").desc_nulls_first())
w6 = Window.orderBy(F.col("first_known_salary_rounded").desc())

filtered = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-03-20").cast("date"))
    .select(
        "id",
        "email",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.coalesce("job_title", F.lit("Unknown")).alias("job_title_filled"),
    )
)

base = (
    filtered
    .filter(F.col("hire_date") >= F.lit("2023-05-25").cast("date"))
    .withColumn("level_job_title_key", F.concat_ws("/", "level", "job_title"))
    .withColumn("level_filled", F.coalesce("level", F.lit("DIRECT")))
)

filtered_employees = (
    base
    .filter(F.col("hire_date") >= F.to_date(F.lit("2023-06-24")))
    .select(
        "*",
        F.round("salary", 1).alias("salary_rounded"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("job_title_filled_2"),
    )
)

scored = (
    filtered_employees
    .filter(F.col("level") != "L1")
    .select(
        "id",
        "job_title",
        "hire_date",
        "job_title_filled",
        "level_job_title_key",
        "level_filled",
        "salary_rounded",
        F.first("salary_rounded", ignorenulls=True).over(w).alias("first_known_salary_rounded"),
        F.sum("salary").over(w2).alias("cumulative_salary"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

kept_keys = (
    scored
    .select("id")
    .subtract(
        df_employees
        .filter(F.col("manager_id").isNotNull())
        .select("id")
    )
)

verified = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("job_title").isNotNull())
    .withColumn("job_title_filled_2", F.coalesce("job_title", F.lit("N/A")))
)

metrics = (
    cleaned
    .select(
        "job_title",
        "job_title_filled",
        "level_job_title_key",
        "level_filled",
        "salary_rounded",
        "cumulative_salary",
        "seq_no",
        F.dense_rank().over(w_hist).alias("seq_no_dense_rank"),
        F.cume_dist().over(w5).alias("cumulative_salary_cume_dist"),
        F.dense_rank().over(w6).alias("first_known_salary_rounded_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "job_title",
        "job_title_filled",
        "level_job_title_key",
        "level_filled",
        "salary_rounded",
        "cumulative_salary",
        "seq_no",
        "seq_no_dense_rank",
        "cumulative_salary_cume_dist",
    )
    .orderBy(F.col("job_title").desc_nulls_first())
)
