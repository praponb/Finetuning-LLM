from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "dept_id").orderBy(F.col("hire_date").asc_nulls_first(), "id")

latest = (
    df_employees
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest
    .withColumn("salary_tier", F.when(F.col("salary") >= 145000, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["job_title", "salary_tier"], ["job_title"]],
        "job_title",
        "salary_tier",
    )
    .agg(
        F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"),
        F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"),
        F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_count"),
    )
    .filter(F.col("engineer_count") < 1000)
    .orderBy(F.col("job_title").desc_nulls_last())
)
