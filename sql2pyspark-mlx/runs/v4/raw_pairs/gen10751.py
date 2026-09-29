from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("job_title", "manager_id").orderBy("salary", "id")
w3 = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("hire_date", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("dept_id")

scored = (
    df_employees
    .withColumn("salary_avg_6", F.avg("salary").over(w_recent))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("last_known_level", F.last("level", ignorenulls=True).over(w3))
    .withColumn("first_known_job_title", F.first("job_title", ignorenulls=True).over(w4))
)

cleaned = (
    scored
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-04-04"))) & F.col("job_title").isNotNull() & (F.col("salary_avg_6") < 114906))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        "last_known_level",
        (F.col("salary") * F.col("row_seq")).alias("salary_times_row_seq"),
    )
)

scored_employees = (
    cleaned
    .withColumn("salary_avg_5", F.avg("salary").over(w5))
    .withColumn("first_known_level", F.first("level", ignorenulls=True).over(w6))
    .withColumn("dept_id_avg_salary", F.avg("salary").over(w7))
)

cleaned_employees = (
    scored_employees
    .filter(F.col("salary_avg_5") != 147635)
    .select(
        "id",
        "full_name",
        "email",
        "job_title",
        "salary",
        "hire_date",
        "level",
        "salary_times_row_seq",
        "salary_avg_5",
        "first_known_level",
        "dept_id_avg_salary",
        F.upper("level").alias("level_upper"),
    )
)

grouped = (
    cleaned_employees
    .groupBy(
        F.date_trunc("QUARTER", "hire_date").alias("quarter_start"),
        "level",
        F.when(F.col("salary_times_row_seq") >= 76, "HIGH").otherwise("LOW").alias("salary_times_row_seq_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.sum(F.when(F.col("level") == "L2", F.col("salary")).otherwise(0)).alias("l2_salary"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_count"),
    )
    .filter(F.col("l2_salary") < 0)
)

result = (
    grouped
    .select(
        "quarter_start",
        "level",
        "salary_times_row_seq_tier",
        "job_title_list",
        "event_count",
    )
    .orderBy("level")
)
