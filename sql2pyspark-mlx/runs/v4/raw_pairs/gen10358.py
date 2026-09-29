from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("job_title_length").desc(), "id")
w2 = Window.partitionBy("manager_id").orderBy("salary", "id")
w3 = Window.partitionBy("level", "dept_id")
w4 = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")

filtered = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2024-03-08"))) & F.col("job_title").isNotNull())
    .withColumn("salary_rounded", F.round("salary", 2))
    .withColumn("job_title_length", F.length("job_title"))
)

scored = (
    filtered
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("next_job_title_length", F.lead("job_title_length", 1).over(w2))
)

valid = (
    scored
    .filter((F.col("hire_date") >= F.to_date(F.lit("2024-02-15"))) & F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        "job_title_length",
        "event_seq",
        F.col("salary").cast("int").alias("salary_int"),
        F.coalesce("job_title", F.lit("UNKNOWN")).alias("job_title_clean"),
    )
)

scored_employees = (
    valid
    .select(
        "*",
        F.min("job_title_length").over(w3).alias("level_min_job_title_length"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

result = (
    scored_employees
    .groupBy("manager_id")
    .pivot("level", ["L3", "L1"])
    .agg(
        F.avg("level_min_job_title_length").alias("value_sum"),
        F.count(F.lit(1)).alias("n"),
    )
    .select(
        "manager_id",
        F.col("L3_value_sum").alias("l3_value_sum"),
        F.col("L3_n").alias("l3_n"),
        F.col("L1_value_sum").alias("l1_value_sum"),
        F.col("L1_n").alias("l1_n"),
    )
)
