from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id")
w2 = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc_nulls_last(), "id")

filtered = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .withColumn("job_title_prefix", F.substring("job_title", 1, 3))
)

latest = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "full_name", "job_title", "hire_date", "level")
)

ranked = (
    latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select("id", "full_name", "job_title", "hire_date")
)

base = (
    ranked
    .filter(F.month("hire_date").isin([6, 7, 11]))
    .select(
        "*",
        F.lower("job_title").alias("job_title_lower"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("job_title_clean"),
    )
)

deduped = (
    base
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("id", "hire_date", "job_title_clean")
)

result = (
    deduped
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("activity_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("activity_month").desc_nulls_last())
)
