from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.partitionBy("level").orderBy(F.col("salary").desc())
w_part = Window.orderBy("salary", "id")
w_hist_2 = Window.partitionBy("level", "dept_id").orderBy(F.col("salary").desc())
w5 = Window.partitionBy("manager_id", "dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("job_title").orderBy(F.col("greatest_salary_salary_dense_rank").desc_nulls_first(), "id")
w7 = Window.partitionBy("location")
w8 = Window.partitionBy("job_title", "level").orderBy("event_seq_2", "id")
w9 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("dept_name")
w11 = Window.partitionBy("job_title")

scored = (
    df_employees
    .select(
        "*",
        F.row_number().over(w_hist).alias("event_seq"),
        F.dense_rank().over(w2).alias("salary_dense_rank"),
        F.row_number().over(w_part).alias("event_seq_2"),
        F.dense_rank().over(w_hist_2).alias("salary_dense_rank_2"),
        F.first("job_title", ignorenulls=True).over(w5).alias("first_known_job_title"),
    )
)

filtered = (
    scored
    .filter(F.col("level").isNotNull() & (F.col("salary") >= 140000))
    .select(
        "*",
        F.greatest("salary", "salary_dense_rank").alias("greatest_salary_salary_dense_rank"),
        F.length("full_name").alias("full_name_length"),
        F.coalesce("job_title", F.lit("N/A")).alias("job_title_clean"),
    )
)

latest = (
    filtered
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

joined = (
    latest
    .alias("s")
    .join(df_departments.alias("p"), F.col("s.dept_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.location"), F.col("p.dept_name"))
)

scored_employees = (
    joined
    .withColumn("location_count_full_name_length", F.count("full_name_length").over(w7))
    .withColumn("event_seq_2_bucket", F.ntile(5).over(w8))
    .withColumn("last_known_greatest_salary_salary_dense_rank", F.last("greatest_salary_salary_dense_rank", ignorenulls=True).over(w9))
)

remaining_keys = (
    scored_employees
    .select("manager_id", "id")
    .intersect(
        df_employees
        .filter(F.col("hire_date") <= "2024-06-01")
        .select("manager_id", "id")
    )
)

reconciled = (
    scored_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "*",
        F.count("full_name_length").over(w10).alias("dept_name_count_full_name_length"),
        F.round((F.col("salary_dense_rank") / F.sum("salary_dense_rank").over(w11)), 4).alias("salary_dense_rank_share"),
        F.min("salary").over(w11).alias("job_title_min_salary"),
    )
)
