from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "level").orderBy(F.col("hire_date").desc())
w2 = Window.partitionBy("dept_id", "job_title").orderBy("salary_plus_row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("row_num_per_is_remote_flag_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-03-24").cast("date"))
    .withColumn("is_remote_label", F.when(F.col("is_remote"), "Yes").otherwise("No"))
    .withColumn("is_remote_flag", F.col("is_remote").cast("int"))
)

deduped = (
    filtered
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

filtered_employees = (
    deduped
    .filter(F.col("hire_date") >= F.lit("2023-04-18").cast("date"))
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote_label",
        "is_remote_flag",
        "row_num",
        F.trim("job_title").alias("clean_job_title"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("resolved_level"),
    )
)

valid = (
    filtered_employees
    .filter(F.col("salary") <= 97000)
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote_label",
        "is_remote_flag",
        "row_num",
        "resolved_level",
        (F.col("row_num") + F.col("salary")).alias("row_num_plus_salary"),
    )
)

filtered_employees_2 = (
    valid
    .filter(F.col("row_num_plus_salary").between(39, 51))
    .withColumn("row_num_per_is_remote_flag", (F.col("row_num") / F.nullif("is_remote_flag", F.lit(0))))
    .withColumn("resolved_job_title", F.coalesce("job_title", F.lit("NONE")))
)

filtered_employees_3 = (
    filtered_employees_2
    .filter(F.col("salary").between(125000, 144000))
    .withColumn("row_num_per_is_remote_flag_int", F.col("row_num_per_is_remote_flag").cast("int"))
    .withColumn("salary_plus_row_num", (F.col("salary") + F.col("row_num")))
    .withColumn("job_title_clean", F.coalesce("job_title", F.lit("OTHER")))
)

with_windows = (
    filtered_employees_3
    .withColumn("cumulative_row_num_per_is_remote_flag", F.sum("row_num_per_is_remote_flag").over(w2))
    .withColumn("cumulative_row_num_per_is_remote_flag_2", F.sum("row_num_per_is_remote_flag").over(w3))
    .withColumn("cumulative_salary_plus_row_num", F.sum("salary_plus_row_num").over(w4))
)

result = (
    with_windows
    .groupBy(
        "manager_id",
        F.when(F.col("salary_plus_row_num") >= 82, "HIGH").otherwise("LOW").alias("salary_plus_row_num_tier"),
        F.when(F.col("cumulative_row_num_per_is_remote_flag_2") >= 391, "PREMIUM").otherwise("STANDARD").alias("cumulative_row_num_per_is_remote_flag_2_tier"),
    )
    .agg(
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.percentile_approx("cumulative_row_num_per_is_remote_flag", 0.5).alias("cumulative_row_num_per_is_remote_flag_p50"),
        F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"),
    )
)
