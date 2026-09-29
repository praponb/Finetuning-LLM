from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").asc_nulls_last(), "id")
w2 = Window.partitionBy("dept_id", "level").orderBy("hire_date", "id")
w3 = Window.partitionBy("job_title").orderBy("salary", "id")

cleaned = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .select(
        "*",
        F.length("email").alias("email_length"),
        F.initcap(F.trim("full_name")).alias("full_name_title"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("resolved_job_title"),
    )
)

deduped = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    deduped
    .withColumn("next_salary", F.lead("salary", 1).over(w2))
    .withColumn("prev_email_length", F.lag("email_length", 1, 0).over(w3))
)

result = (
    enriched
    .groupBy("job_title")
    .agg(
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.sum(F.when(F.col("job_title") == "Director", F.col("salary"))).alias("director_total_salary"),
    )
    .orderBy(F.col("job_title").desc())
)
