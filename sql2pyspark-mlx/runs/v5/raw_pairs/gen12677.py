from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("job_title").orderBy(F.col("salary").desc(), "id")

valid = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.coalesce("full_name", F.lit("N/A")).alias("full_name_filled"),
    )
)

ranked = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "job_title", "salary", "full_name_filled")
)

latest = (
    ranked
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("id", "job_title", "salary", "full_name_filled")
)

cleaned = (
    latest
    .filter(F.col("job_title").isNotNull())
    .select(
        "*",
        F.col("salary").cast("int").alias("salary_int"),
        F.round((F.col("salary") * 1.07), 2).alias("salary_scaled"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("salary") >= 155000, "PREMIUM").otherwise("STANDARD").alias("salary_tier"),
    )
    .agg(
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.percentile_approx("salary_scaled", [0.25, 0.5, 0.75]).alias("salary_scaled_quartiles"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
    )
)
