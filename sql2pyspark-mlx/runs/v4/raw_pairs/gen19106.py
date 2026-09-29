from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary", "id")
w_part = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc(), "id")
w3 = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_employees
    .filter(F.col("salary") > 148000)
    .withColumn("email_normalized", F.upper(F.trim("email")))
)

enriched = (
    cleaned
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("seq_no", F.row_number().over(w_part))
    .withColumn("cumulative_salary", F.sum("salary").over(w3))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id") == F.col("d.id"), "left")
    .select(
        "e.*",
        F.col("d.job_title").alias("employee_job_title"),
        F.col("d.full_name").alias("employee_full_name"),
    )
)

base = (
    with_dims
    .filter((F.col("hire_date") >= F.lit("2023-05-28").cast("date")) & (F.col("level") != "L3"))
    .select(
        "id",
        "full_name",
        "email",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "event_seq",
        "seq_no",
        "cumulative_salary",
        "employee_job_title",
        "employee_full_name",
        F.greatest("cumulative_salary", "seq_no").alias("greatest_cumulative_salary_seq_no"),
        F.concat_ws(",", "email", "full_name").alias("email_full_name_key"),
    )
)

result = (
    base
    .groupBy(F.date_trunc("week", "hire_date").alias("week_start"))
    .agg(
        F.max("event_seq").alias("max_event_seq"),
        F.avg(F.when(F.col("employee_job_title") == "Analyst", F.col("seq_no"))).alias("analyst_avg_seq_no"),
        F.sum(F.when(F.col("level") == "L5", F.col("event_seq")).otherwise(0)).alias("l5_event_seq"),
        F.countDistinct("job_title").alias("distinct_job_titles"),
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_rows"),
        F.count_if(F.col("job_title") == "Engineer").alias("engineer_events"),
    )
    .orderBy(F.col("engineer_events").desc())
)
