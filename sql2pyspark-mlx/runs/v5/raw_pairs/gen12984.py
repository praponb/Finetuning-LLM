from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("dept_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("job_title").orderBy("next_salary", "id")

base = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-02-10"))) & F.col("full_name").like("%smit%"))
    .select(
        "*",
        F.when(F.col("job_title").isin(["Manager", "Analyst"]), "Core").otherwise("Other").alias("job_title_group"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)

valid = (
    base
    .filter(F.col("email").like("%@GMAIL.COM"))
    .withColumn("email_level_key", F.concat_ws("/", "email", "level"))
)

metrics = (
    valid
    .filter(F.col("job_title") == "Intern")
    .select(
        "*",
        F.lag("salary").over(w_grp).alias("prev_salary"),
        F.lead("salary").over(w_grp).alias("next_salary"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

cleaned = (
    metrics
    .filter(F.col("hire_date") > F.to_date(F.lit("2023-07-09")))
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
        "job_title_group",
        "is_remote_label",
        "email_level_key",
        "prev_salary",
        "next_salary",
        "event_seq",
        F.date_trunc("month", "hire_date").alias("hire_date_period"),
        F.coalesce("level", F.lit("OTHER")).alias("level_clean"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "is_remote",
        "job_title_group",
        "email_level_key",
        "next_salary",
        F.lag("next_salary").over(w2).alias("prev_next_salary"),
        F.lead("next_salary").over(w2).alias("next_next_salary"),
        F.lead("event_seq").over(w2).alias("next_event_seq"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
