from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc(), "id")
w3 = Window.orderBy("hire_date", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("manager_id").orderBy("rank_in_group", "id")
w5 = Window.partitionBy("manager_id").orderBy("salary", "id")
w_grp = Window.partitionBy("job_title").orderBy("hire_date", "id")
w7 = Window.partitionBy("manager_id").orderBy("salary_avg_5_prior", "id")

deduped = (
    df_employees
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    deduped
    .withColumn("rank_in_group_2", F.row_number().over(w2))
    .filter(F.col("rank_in_group_2") == 1)
)

metrics = (
    latest
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        F.avg("salary").over(w3).alias("salary_avg_5_prior"),
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w5).alias("row_seq_2"),
    )
)

with_windows = (
    metrics
    .filter(F.col("manager_id").isNull())
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "is_remote",
        "salary_avg_5_prior",
        "row_seq_2",
        F.row_number().over(w_grp).alias("row_seq_3"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "salary_avg_5_prior",
        "row_seq_3",
        "event_seq",
        F.lag("row_seq_2").over(w7).alias("prev_row_seq_2"),
        F.lead("row_seq_2").over(w7).alias("next_row_seq_2"),
        F.lead("salary_avg_5_prior").over(w7).alias("next_salary_avg_5_prior"),
        F.lag("salary_avg_5_prior").over(w7).alias("prev_salary_avg_5_prior"),
        F.row_number().over(w7).alias("event_seq_2"),
    )
)

filtered = (
    enriched
    .filter(F.col("full_name").like("O%"))
    .select("*", F.substring("job_title", 1, 4).alias("job_title_prefix"))
)

result = (
    filtered
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "row_seq_3",
        "event_seq",
        "next_row_seq_2",
        "next_salary_avg_5_prior",
    )
    .orderBy(F.col("id").asc_nulls_first())
)
