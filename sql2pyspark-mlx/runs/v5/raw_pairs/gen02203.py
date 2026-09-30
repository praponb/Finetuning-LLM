from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary", "id")
w2 = Window.partitionBy("level").orderBy("dedup_rank")

valid = (
    df_employees
    .filter(F.col("level").isNotNull() & (~F.col("job_title").eqNullSafe(F.lit("Intern"))))
    .select(
        "id",
        "full_name",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.lower("level").alias("level_lower"),
    )
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

deduped = (
    ranked
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

base = (
    deduped
    .filter(F.month("hire_date").isin([1, 7, 8]) & F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "level_lower",
        "dedup_rank",
        "rank_in_group",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
)

result = (
    base
    .rollup("level")
    .agg(
        F.min("salary").alias("min_salary"),
        F.min("salary").alias("min_salary_2"),
        F.count("*").alias("num_rows"),
        F.grouping_id().alias("grouping_id"),
    )
)
