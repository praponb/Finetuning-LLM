from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "dept_id").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.partitionBy("manager_id", "level").orderBy(F.col("hire_date").desc(), "id")
w3 = Window.partitionBy("job_title").orderBy("hire_date_day", "id")
w4 = Window.orderBy("event_seq", "id")
w5 = Window.partitionBy("job_title").orderBy("hire_date_day", "id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_employees
    .filter(F.col("job_title") != "Director")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "level",
        F.dayofmonth("hire_date").alias("hire_date_day"),
        F.coalesce("level", F.lit("DIRECT")).alias("level_filled"),
    )
)

with_windows = (
    filtered
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)

metrics = (
    with_windows
    .withColumn("seq_no_2", F.row_number().over(w3))
    .withColumn("event_seq_bucket", F.ntile(4).over(w4))
)

result = (
    metrics
    .withColumn("event_seq_bucket_avg_2", F.avg("event_seq_bucket").over(w5))
    .withColumn("hire_date_day_bucket", F.ntile(3).over(w3))
)
