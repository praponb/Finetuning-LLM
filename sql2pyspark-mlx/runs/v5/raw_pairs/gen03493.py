from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id")
w2 = Window.partitionBy("dept_id").orderBy("salary", "id")
w3 = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("job_title").orderBy("prev_salary", "id")
w5 = Window.partitionBy("job_title").orderBy("prev_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("job_title").orderBy("salary", "id")

enriched = (
    df_employees
    .filter(F.col("is_remote"))
    .select(
        "id",
        "full_name",
        "email",
        "job_title",
        "salary",
        "hire_date",
        F.lag("salary", 1, 0).over(w).alias("prev_salary"),
        F.lead("salary", 2).over(w2).alias("next_salary"),
        F.avg("salary").over(w3).alias("salary_avg_6"),
    )
)

with_windows = (
    enriched
    .withColumn("prev_salary_bucket", F.ntile(10).over(w4))
    .withColumn("first_known_job_title", F.first("job_title", ignorenulls=True).over(w5))
    .withColumn("event_seq", F.row_number().over(w6))
)

base = (
    with_windows
    .filter(F.length("email") > 12)
    .select(
        "id",
        "full_name",
        "email",
        "salary",
        "hire_date",
        "prev_salary",
        "event_seq",
        (F.col("event_seq") + F.col("next_salary")).alias("event_seq_plus_next_salary"),
        F.coalesce("next_salary", F.lit(0)).alias("next_salary_filled"),
    )
)

filtered = (
    base
    .filter(F.col("prev_salary") <= 140000)
    .select(
        "id",
        "full_name",
        "email",
        "salary",
        "hire_date",
        "prev_salary",
        "event_seq",
        "event_seq_plus_next_salary",
        "next_salary_filled",
        F.when(F.col("next_salary_filled") >= 83, "High").when(F.col("next_salary_filled") >= 17, "Medium").otherwise("Low").alias("next_salary_filled_tier"),
    )
)

matched_keys = (
    filtered
    .select("id")
    .subtract(
        df_employees
        .filter(F.col("is_remote"))
        .select("id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("event_seq_plus_next_salary") >= 84, "LARGE").otherwise("SMALL").alias("event_seq_plus_next_salary_tier"),
    )
    .agg(
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.round(F.avg("next_salary_filled"), 2).alias("avg_next_salary_filled"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
