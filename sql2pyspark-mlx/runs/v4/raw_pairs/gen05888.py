from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "job_title").orderBy(F.col("email_length").desc(), "id")
w2 = Window.partitionBy("level").orderBy(F.col("dedup_rank").desc(), "id")

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2024-01-23").cast("date")) & F.col("manager_id").isNotNull() & F.col("email").isNull())
    .select(
        "*",
        F.length("email").alias("email_length"),
        F.when(F.col("salary") >= 140000, "High").when(F.col("salary") >= 60000, "Medium").otherwise("Low").alias("salary_level"),
        F.coalesce("level", F.lit("DIRECT")).alias("resolved_level"),
    )
)

latest = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

ranked = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "is_remote",
        "level",
        "salary_level",
        "rank_in_group",
    )
)
