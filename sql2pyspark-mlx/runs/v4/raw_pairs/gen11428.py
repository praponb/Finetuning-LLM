from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc(), "id")
w3 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("job_title", "level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("manager_id", "level").orderBy("hire_date", "id")
w6 = Window.partitionBy("level").orderBy("seq_no", "id")
w7 = Window.partitionBy("level").orderBy(F.col("last_known_row_seq").desc(), "id")
w8 = Window.partitionBy("job_title", "level").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-06-29").cast("date")) & F.col("job_title").isNotNull() & F.col("full_name").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.upper("level").alias("level_upper"),
        F.coalesce("job_title", F.lit("NONE")).alias("resolved_job_title"),
    )
)

metrics = (
    cleaned
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("last_known_job_title", F.last("job_title", ignorenulls=True).over(w3))
)

enriched = (
    metrics
    .select(
        "*",
        F.last("row_seq", ignorenulls=True).over(w4).alias("last_known_row_seq"),
        F.row_number().over(w5).alias("row_seq_2"),
        F.lead("row_seq", 1).over(w6).alias("next_row_seq"),
    )
)

result = (
    enriched
    .select(
        "*",
        F.row_number().over(w7).alias("event_seq"),
        F.sum("next_row_seq").over(w8).alias("cumulative_next_row_seq"),
    )
)
