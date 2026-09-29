from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("email_length").asc_nulls_last(), "id")
w2 = Window.partitionBy("level").orderBy(F.col("rank_in_group").desc(), "id")
w3 = Window.partitionBy("job_title").orderBy("salary", "id")
w4 = Window.partitionBy("dept_name", "location").orderBy(F.col("row_seq").desc(), "id")
w5 = Window.partitionBy("dept_name", "manager_id")
w6 = Window.partitionBy("job_title").orderBy("hire_date", "id")
w7 = Window.partitionBy("manager_id")
w8 = Window.partitionBy("location", "job_title").orderBy(F.col("prev_email_length").desc(), "id")

with_dims = (
    df_employees
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.location"), F.col("d.dept_name"))
)

cleaned = (
    with_dims
    .filter(F.length("full_name") >= 14)
    .withColumn("clean_level", F.trim("level"))
    .withColumn("email_length", F.length("email"))
    .withColumn("job_title_clean", F.coalesce("job_title", F.lit("UNKNOWN")))
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    deduped
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("prev_email_length", F.lag("email_length", 1).over(w3))
)

enriched = (
    with_windows
    .select(
        "*",
        F.row_number().over(w4).alias("row_seq_2"),
        F.min("prev_email_length").over(w5).alias("dept_name_min_prev_email_length"),
        F.lag("row_seq", 1, 0).over(w6).alias("prev_row_seq"),
        F.max("prev_email_length").over(w7).alias("manager_id_max_prev_email_length"),
        F.ntile(4).over(w8).alias("prev_email_length_bucket"),
    )
)

result = (
    enriched
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "hire_date",
        "is_remote",
        "dept_name",
        "clean_level",
        "email_length",
    )
    .orderBy(F.col("dept_id").desc_nulls_last())
)
