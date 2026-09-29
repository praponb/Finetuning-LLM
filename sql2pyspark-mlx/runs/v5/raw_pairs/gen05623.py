from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("budget").desc(), "id")
w3 = Window.partitionBy("location").orderBy(F.col("running_dept_name_length").desc_nulls_first(), "id")
w_hist = Window.orderBy("dept_name_length", "id")
w_running = Window.orderBy("dept_name_length_bucket", "id")
w6 = Window.orderBy("next_dept_name_length", "id").rowsBetween(-2, -1)
w7 = Window.orderBy(F.col("dept_name_length").desc(), "id")
w8 = Window.orderBy("event_seq", "id")

valid = (
    df_departments
    .filter(F.col("budget").between(2862000, 2934000))
    .select(
        "*",
        F.trim("location").alias("clean_location"),
        F.length("dept_name").alias("dept_name_length"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "location",
        "budget",
        "clean_location",
        "dept_name_length",
        F.sum("dept_name_length").over(w).alias("running_dept_name_length"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select("id", "dept_name_length")
)

metrics_departments = (
    deduped
    .filter(F.col("dept_name_length") != 55)
    .select(
        "id",
        "dept_name_length",
        F.ntile(4).over(w_hist).alias("dept_name_length_bucket"),
        F.lag("dept_name_length", 1).over(w_hist).alias("prev_dept_name_length"),
    )
)

enriched = (
    metrics_departments
    .select(
        "id",
        "dept_name_length",
        "prev_dept_name_length",
        F.lag("dept_name_length_bucket").over(w_running).alias("prev_dept_name_length_bucket"),
        F.lead("dept_name_length").over(w_running).alias("next_dept_name_length"),
        F.row_number().over(w_running).alias("event_seq"),
    )
)

metrics_departments_2 = (
    enriched
    .withColumn("next_dept_name_length_avg_2_prior", F.avg("next_dept_name_length").over(w6))
    .withColumn("event_seq_2", F.row_number().over(w7))
    .withColumn("prev_next_dept_name_length", F.lag("next_dept_name_length", 1).over(w8))
)

result = (
    metrics_departments_2
    .select("id", "dept_name_length", "event_seq")
    .orderBy("id", "dept_name_length")
)
