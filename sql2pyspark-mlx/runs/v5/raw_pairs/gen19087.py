from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("salary").desc())
w2 = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w3 = Window.partitionBy("job_title", "level").orderBy(F.col("hire_date").desc(), "id")
w4 = Window.partitionBy("job_title", "level").orderBy(F.col("rank_in_group").desc(), "id")

filtered = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .select(
        "*",
        F.lower("email").alias("email_lower"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("level_filled"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "email",
        "job_title",
        "salary",
        "hire_date",
        "level",
        "email_lower",
        "level_filled",
    )
)

latest_employees = (
    latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

latest_employees_2 = (
    latest_employees
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

ranked = (
    latest_employees_2
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .select("id", "job_title", "salary")
)

result = (
    ranked
    .rollup("job_title")
    .agg(
        F.percentile_approx("salary", 0.5).alias("salary_p50"),
        F.max("salary").alias("max_salary"),
        F.grouping("job_title").alias("gid"),
    )
    .orderBy(F.col("salary_p50").desc())
)
