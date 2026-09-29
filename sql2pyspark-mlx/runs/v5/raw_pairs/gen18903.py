from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("job_title").orderBy(F.col("salary").desc())

cleaned = (
    df_employees
    .filter(F.col("job_title").isNotNull() & (F.year("hire_date") >= 2023))
    .select(
        "id",
        "full_name",
        "email",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.trim("full_name").alias("clean_full_name"),
        F.coalesce("level", F.lit("OTHER")).alias("level_clean"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "full_name",
        "email",
        "job_title",
        "salary",
        "hire_date",
        F.sum("salary").over(w).alias("cumulative_salary"),
        F.last("job_title", ignorenulls=True).over(w2).alias("last_known_job_title"),
    )
)

cleaned_employees = (
    scored
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-05-04"))) & F.col("email").isNull())
    .withColumn("cumulative_salary_str", F.col("cumulative_salary").cast("double"))
    .withColumn("job_title_filled", F.coalesce("job_title", F.lit("N/A")))
)

ranked = (
    cleaned_employees
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

report = (
    ranked
    .select(
        "id",
        "email",
        "salary",
        "hire_date",
        "cumulative_salary",
        "last_known_job_title",
        "cumulative_salary_str",
        "rank_in_group",
    )
    .orderBy(F.col("cumulative_salary_str").desc_nulls_first())
)

result = (
    report
    .select(
        "id",
        "hire_date",
        "cumulative_salary",
        "last_known_job_title",
        "cumulative_salary_str",
    )
    .orderBy(F.col("hire_date").desc())
)
