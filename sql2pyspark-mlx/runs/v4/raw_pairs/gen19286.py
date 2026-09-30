from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("salary_tier").orderBy(F.col("row_count").desc())

valid = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .select(
        "*",
        F.initcap(F.trim("email")).alias("email_title"),
        F.coalesce("email", F.lit("Unknown")).alias("email_filled"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("resolved_level"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("salary") >= 121000, "PREMIUM").otherwise("STANDARD").alias("salary_tier"),
        "level",
    )
    .agg(
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.count("*").alias("row_count"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .withColumn("row_count_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("salary_tier").desc())
)
