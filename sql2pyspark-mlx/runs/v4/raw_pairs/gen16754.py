from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id")

ranked = (
    df_employees
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("salary") >= 123000, "LARGE").otherwise("SMALL").alias("salary_tier"),
    )
    .agg(
        F.countDistinct("job_title").alias("distinct_job_titles"),
        F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.percentile_approx("salary", 0.5).alias("salary_p50"),
        F.avg(F.when(F.col("job_title") == "Director", F.col("salary"))).alias("director_avg_salary"),
        F.countDistinct("job_title").alias("distinct_job_titles_2"),
    )
    .orderBy(F.col("intern_count").desc_nulls_last())
)
