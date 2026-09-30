from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("salary").desc())
w2 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(-2, -1)
w3 = Window.orderBy("salary")
w4 = Window.partitionBy("job_title").orderBy("salary_dense_rank", "id")
w5 = Window.orderBy("salary_rounded")

cleaned = (
    df_employees
    .filter(F.lower("email").like("%ack%"))
    .select(
        "*",
        F.when(F.col("salary") >= 155000, "High").when(F.col("salary") >= 86000, "Medium").otherwise("Low").alias("salary_tier"),
        F.round("salary", 2).alias("salary_rounded"),
        F.coalesce("job_title", F.lit("OTHER")).alias("job_title_clean"),
    )
)

cleaned_employees = (
    cleaned
    .filter((F.col("hire_date") > F.lit("2023-05-12").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "salary_tier",
        "salary_rounded",
        "job_title_clean",
        F.last_day("hire_date").alias("hire_date_month_end"),
    )
)

enriched = (
    cleaned_employees
    .withColumn("salary_dense_rank", F.dense_rank().over(w))
    .withColumn("salary_avg_2_prior", F.avg("salary").over(w2))
)

metrics = (
    enriched
    .select(
        "*",
        F.cume_dist().over(w3).alias("salary_cume_dist"),
        F.ntile(5).over(w4).alias("salary_dense_rank_bucket"),
        F.cume_dist().over(w5).alias("salary_rounded_cume_dist"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("salary_rounded") >= 71, "PREMIUM").otherwise("STANDARD").alias("salary_rounded_tier"),
        "level",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.stddev_samp("salary").alias("salary_stddev"),
        F.sum(F.when(F.col("job_title") == "Intern", F.col("salary_avg_2_prior")).otherwise(0)).alias("intern_salary_avg_2_prior"),
    )
    .filter(F.col("intern_salary_avg_2_prior") < 1000000)
    .orderBy(F.col("salary_rounded_tier").desc_nulls_first())
)
