from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("job_title").orderBy("email_length", "id")
w2 = Window.orderBy("salary", "id")
w3 = Window.partitionBy("job_title").orderBy("prev_email_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_id").orderBy("next_email_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("manager_id")
w6 = Window.orderBy(F.col("next_salary").desc(), "id")
w7 = Window.partitionBy("dept_id", "job_title").orderBy("prev_prev_salary")
w8 = Window.orderBy("seq_no_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("job_title").orderBy(F.col("first_known_seq_no").desc())
w10 = Window.partitionBy("location").orderBy("running_seq_no_2", "id")

filtered = (
    df_employees
    .filter(F.col("manager_id") == 11)
    .withColumn("email_length", F.length("email"))
)

metrics = (
    filtered
    .withColumn("prev_email_length", F.lag("email_length").over(w_recent))
    .withColumn("next_email_length", F.lead("email_length").over(w_recent))
    .withColumn("prev_salary", F.lag("salary").over(w_recent))
    .withColumn("next_salary", F.lead("salary").over(w_recent))
    .withColumn("seq_no", F.row_number().over(w_recent))
)

scored = (
    metrics
    .filter(F.col("prev_salary") > 69000)
    .select(
        "id",
        "dept_id",
        "job_title",
        "email_length",
        "prev_email_length",
        "prev_salary",
        F.lag("prev_salary", 2).over(w2).alias("prev_prev_salary"),
        F.first("seq_no", ignorenulls=True).over(w3).alias("first_known_seq_no"),
        F.last("prev_email_length", ignorenulls=True).over(w4).alias("last_known_prev_email_length"),
        F.sum("prev_email_length").over(w5).alias("manager_id_sum_prev_email_length"),
        F.row_number().over(w6).alias("seq_no_2"),
    )
)

deduped = (
    scored
    .withColumn("row_num", F.rank().over(w7))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    deduped
    .alias("a")
    .join(df_departments.alias("b"), F.col("a.dept_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.dept_name"), F.col("b.budget"), F.col("b.location"))
)

metrics_employees = (
    enriched
    .select(
        "id",
        "dept_id",
        "email_length",
        "last_known_prev_email_length",
        "manager_id_sum_prev_email_length",
        "seq_no_2",
        "location",
        F.sum("seq_no_2").over(w8).alias("running_seq_no_2"),
        F.rank().over(w9).alias("first_known_seq_no_rank"),
    )
)

latest = (
    metrics_employees
    .withColumn("rank_in_group", F.row_number().over(w10))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select(
        "id",
        "dept_id",
        "email_length",
        "manager_id_sum_prev_email_length",
        "location",
        "running_seq_no_2",
        "first_known_seq_no_rank",
        "rank_in_group",
    )
    .orderBy(F.col("location").desc())
)
