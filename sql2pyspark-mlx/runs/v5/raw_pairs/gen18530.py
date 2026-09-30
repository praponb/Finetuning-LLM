from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("hire_date", "id")
w4 = Window.partitionBy("job_title").orderBy(F.col("employee_hire_date").desc())

base = (
    df_employees
    .filter(F.col("job_title") == "Analyst")
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        F.when(F.col("salary") >= 155000, "High").when(F.col("salary") >= 86000, "Medium").otherwise("Low").alias("salary_tier"),
    )
)

enriched = (
    base
    .filter(F.col("email").like("%@company.co"))
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.last("job_title", ignorenulls=True).over(w2).alias("last_known_job_title"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

enriched_employees = (
    enriched
    .alias("s")
    .join(df_employees.alias("d"), F.col("s.manager_id") == F.col("d.id"), "left")
    .select(
        "s.*",
        F.col("d.hire_date").alias("employee_hire_date"),
        F.col("d.full_name").alias("employee_full_name"),
        F.col("d.job_title").alias("employee_job_title"),
    )
)

deduped = (
    enriched_employees
    .withColumn("rank_in_group", F.rank().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

enriched_employees_2 = (
    deduped
    .alias("s")
    .join(df_employees.alias("e"), F.col("s.manager_id") == F.col("e.id"), "left")
    .select(
        "s.*",
        F.col("e.level"),
        F.col("e.salary"),
        F.col("e.job_title").alias("employee_job_title_2"),
    )
)

cleaned = (
    enriched_employees_2
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-07-15"))) & (F.col("level") == "L5"))
    .withColumn("is_remote_flag", F.col("is_remote").cast("int"))
    .withColumn("is_director", F.when(F.col("job_title") == "Director", 1).otherwise(0))
)

filtered = (
    cleaned
    .filter((F.col("hire_date") >= F.lit("2023-04-01").cast("date")) & (F.col("seq_no") >= 8))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "is_remote",
        "salary_tier",
        "last_known_job_title",
        "seq_no",
        "employee_job_title",
        "rank_in_group",
        "level",
        "salary",
        "is_remote_flag",
        "is_director",
        F.ceil("is_director").alias("is_director_whole"),
    )
)

result = (
    filtered
    .select(
        "id",
        "full_name",
        "manager_id",
        "is_remote_flag",
        "is_director",
        "is_director_whole",
        F.when(F.col("full_name").rlike("^(Emma Brown|Grace Kim)$"), "GROUP_B").otherwise("GROUP_A").alias("full_name_zone"),
    )
)
