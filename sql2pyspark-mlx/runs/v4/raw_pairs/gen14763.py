from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")
w2 = Window.orderBy("salary", "id").rowsBetween(-2, -1)
w3 = Window.orderBy(F.col("hire_date").desc(), "id")
w4 = Window.partitionBy("dept_id", "job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("job_title").orderBy("salary", "id")
w6 = Window.partitionBy("manager_id", "level").orderBy("salary_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("salary_avg_2_prior", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.partitionBy("manager_id").orderBy("seq_no", "id")

valid = (
    df_employees
    .filter(F.col("hire_date") > F.to_date(F.lit("2024-02-19")))
    .select(
        "*",
        F.lower("email").alias("email_lower"),
        F.coalesce("level", F.lit("N/A")).alias("level_filled"),
    )
)

enriched = (
    valid
    .withColumn("salary_bucket", F.ntile(10).over(w))
    .withColumn("salary_avg_2_prior", F.avg("salary").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("running_salary", F.sum("salary").over(w4))
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    latest
    .withColumn("last_known_running_salary", F.last("running_salary", ignorenulls=True).over(w6))
    .withColumn("salary_bucket_avg_2", F.avg("salary_bucket").over(w7))
    .withColumn("seq_no_bucket", F.ntile(3).over(w8))
)

result = (
    scored
    .groupBy(
        F.when(F.col("salary") >= 133000, "PREMIUM").otherwise("STANDARD").alias("salary_tier"),
    )
    .agg(
        F.count_if(F.col("level") == "L1").alias("l1_events"),
        F.countDistinct("job_title").alias("distinct_job_titles"),
        F.countDistinct("job_title").alias("distinct_job_titles_2"),
    )
)
