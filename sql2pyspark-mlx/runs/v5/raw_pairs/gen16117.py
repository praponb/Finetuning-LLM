from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary_whole_plus_salary", "id")
w2 = Window.partitionBy("job_title").orderBy("salary_whole", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.ceil("salary").alias("salary_whole"),
    )
)

filtered = (
    base
    .filter((F.col("hire_date") > F.lit("2023-06-08").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "*",
        (F.col("salary_whole") + F.col("salary")).alias("salary_whole_plus_salary"),
        F.coalesce("level", F.lit("NONE")).alias("level_filled"),
    )
)

base_employees = (
    filtered
    .filter(F.col("salary_whole_plus_salary").isNotNull())
    .select(
        "id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        "salary_whole",
        "salary_whole_plus_salary",
        "level_filled",
        F.substring("full_name", 1, 2).alias("full_name_prefix"),
    )
)

scored = (
    base_employees
    .withColumn("prev_salary", F.lag("salary").over(w))
    .withColumn("next_salary_whole_plus_salary", F.lead("salary_whole_plus_salary").over(w))
    .withColumn("event_seq", F.row_number().over(w))
)

result = (
    scored
    .select(
        "id",
        "salary",
        "hire_date",
        "level_filled",
        "full_name_prefix",
        "next_salary_whole_plus_salary",
        F.avg("salary_whole_plus_salary").over(w2).alias("salary_whole_plus_salary_avg_5_prior"),
        F.last("salary_whole", ignorenulls=True).over(w3).alias("last_known_salary_whole"),
    )
)
